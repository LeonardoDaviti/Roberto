from __future__ import annotations

import inspect
from pathlib import Path

from roberto_app.llm.retrieval import RetrievalContextBuilder
from roberto_app.llm.schemas import DailyDigestAutoBlock
from roberto_app.llm.validation import validate_digest_auto_block, validate_user_auto_block
from roberto_app.notesys.renderer import render_digest_auto_block, render_user_auto_block
from roberto_app.notesys.updater import update_note_file
from roberto_app.pipeline.common import local_now_iso, newest_tweet_id, read_following, run_id_now, utc_now_iso
from roberto_app.pipeline.editorial import normalize_trigger_refs, staging_target_path
from roberto_app.pipeline.briefing import build_daily_briefing, render_briefing
from roberto_app.pipeline.entity_graph import (
    index_entities_from_digest,
    index_entities_from_tweets,
    render_entity_auto_block,
)
from roberto_app.pipeline.human_memory import (
    detect_conflict_cards,
    propose_idea_cards,
    render_conflict_auto_block,
    render_idea_auto_block,
    render_shuffle_auto_block,
    select_shuffle_pack,
    week_key_from_iso,
)
from roberto_app.pipeline.reliability import build_reliability_kernel
from roberto_app.pipeline.report import RunReport
from roberto_app.pipeline.eval import run_eval
from roberto_app.pipeline.search_index import rebuild_search_index
from roberto_app.pipeline.story_memory import persist_stories
from roberto_app.pipeline.taxonomy import load_entity_alias_overrides, load_tag_aliases
from roberto_app.pipeline.uncertainty import to_conflict_nodes
from roberto_app.pipeline.greene import run_chapter_argument_gap_cycle, run_greene_cycle
from roberto_app.sources.refs import x_source_ref
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo
from roberto_app.x_api.client import XClient


def _digest_path(notes_dir: Path, run_id: str, now_local_iso: str) -> Path:
    date_part = now_local_iso[:10]
    time_part = now_local_iso[11:19].replace(":", "")
    return notes_dir / "digests" / f"{date_part}__run-{time_part}.md"


def _call_summarize_user(llm, username: str, tweets: list[dict], *, retrieval_context: list[dict], run_id: str):
    params = inspect.signature(llm.summarize_user).parameters
    if "run_id" in params:
        return llm.summarize_user(username, tweets, retrieval_context=retrieval_context, run_id=run_id)
    return llm.summarize_user(username, tweets, retrieval_context=retrieval_context)


def _call_summarize_digest(
    llm,
    highlights_payload: list[dict],
    new_tweets_payload: dict[str, list[dict]],
    *,
    retrieval_context: list[dict],
    run_id: str,
):
    params = inspect.signature(llm.summarize_digest).parameters
    if "run_id" in params:
        return llm.summarize_digest(
            highlights_payload,
            new_tweets_payload,
            retrieval_context=retrieval_context,
            run_id=run_id,
        )
    return llm.summarize_digest(
        highlights_payload,
        new_tweets_payload,
        retrieval_context=retrieval_context,
    )


def run_v1(settings, repo: StorageRepo, x_client: XClient, llm, *, resume: bool = False) -> RunReport:
    usernames = read_following(settings.resolve("config", "following.txt"))
    reliability = build_reliability_kernel(settings, mode="v1", resume=resume)
    state = reliability.start(usernames, run_id_factory=run_id_now)
    run_id = state.run_id
    started_at = state.started_at
    now_local = local_now_iso(settings.notes.note_timezone)

    report = RunReport(run_id=run_id, mode="v1", started_at=started_at)
    registry_meta = llm.registry_meta() if hasattr(llm, "registry_meta") else {}
    if settings.v17.enabled:
        report.prompt_pack_version = str(registry_meta.get("prompt_pack_version") or settings.v17.prompt_pack_version)
        report.schema_pack_version = str(registry_meta.get("schema_pack_version") or settings.v17.schema_pack_version)
        if registry_meta.get("prompt_pack_hash"):
            report.prompt_pack_hash = str(registry_meta["prompt_pack_hash"])
        if registry_meta.get("schema_pack_hash"):
            report.schema_pack_hash = str(registry_meta["schema_pack_hash"])
    repo.create_run(run_id, "v1", started_at)
    retriever = RetrievalContextBuilder(repo, settings.v4.retrieval)
    notes_root = settings.resolve("notes")
    staging_enabled = settings.v13.enabled
    entity_alias_overrides = load_entity_alias_overrides(settings)
    tag_aliases = load_tag_aliases(settings)

    def _target_path(live_path: Path) -> Path:
        if not staging_enabled:
            return live_path
        return staging_target_path(notes_root, run_id, live_path)

    def _prepare_target_path(live_path: Path) -> Path:
        target = _target_path(live_path)
        if staging_enabled and live_path.exists() and not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(live_path.read_text(encoding="utf-8"), encoding="utf-8")
        return target

    def _track_note(
        *,
        note_type: str,
        live_path: Path,
        target_path: Path,
        live_exists: bool,
        note_updated: bool,
        trigger_refs: list[dict[str, str]],
    ) -> None:
        if staging_enabled:
            repo.upsert_staged_note(
                run_id=run_id,
                live_path=str(live_path),
                staged_path=str(target_path),
                mode="v1",
                note_type=note_type,
                trigger_refs=normalize_trigger_refs(trigger_refs),
                created_at=now_local,
            )
            if str(live_path) not in report.staged_notes:
                report.staged_notes.append(str(live_path))
            if not live_exists and str(live_path) not in report.created_notes:
                report.created_notes.append(str(live_path))
            elif live_exists and note_updated and str(live_path) not in report.updated_notes:
                report.updated_notes.append(str(live_path))
            return

        if not live_exists and str(live_path) not in report.created_notes:
            report.created_notes.append(str(live_path))
        elif live_exists and note_updated and str(live_path) not in report.updated_notes:
            report.updated_notes.append(str(live_path))

    highlights_payload: list[dict] = []
    new_tweets_payload: dict[str, list[dict]] = {}
    valid_digest_refs: set[tuple[str, str]] = set()
    touched_entity_ids: set[str] = set()

    try:
        for idx, username in enumerate(usernames):
            if reliability.should_skip_user(username):
                report.per_user_new_tweets[username] = 0
                reliability.journal.write("user_skipped", username=username, reason="checkpoint_completed")
                continue

            reliability.mark_user_started(username)
            try:
                with repo.transaction(label=f"user_{idx}"):
                    user_row = repo.get_user(username)
                    if not user_row or not user_row.get("user_id"):
                        looked_up = x_client.lookup_user(username)
                        repo.upsert_user(username, looked_up.id, looked_up.name)
                        user_id = looked_up.id
                    else:
                        user_id = str(user_row["user_id"])
                        if not user_row.get("display_name"):
                            repo.upsert_user(username, user_id, user_row.get("display_name"))

                    tweets = x_client.fetch_user_tweets(
                        user_id,
                        since_id=None,
                        max_results=settings.x.max_results,
                        exclude=settings.x.exclude,
                        tweet_fields=settings.x.tweet_fields,
                        max_pages=1,
                    )
                    inserted_count = repo.insert_tweets(username, tweets)
                    report.per_user_new_tweets[username] = inserted_count

                    tweet_ids = [t.id for t in tweets]
                    newest = newest_tweet_id(tweet_ids)
                    existing_last_seen = (repo.get_user(username) or {}).get("last_seen_tweet_id")
                    repo.update_user_state(username, newest or existing_last_seen, utc_now_iso())

                    recent_tweets = repo.get_recent_tweets(username, limit=settings.pipeline.v1.backfill_count)
                    user_context = retriever.user_context(
                        username,
                        recent_tweets,
                        focus_tweet_ids={str(t.id) for t in tweets},
                    )
                    summary = _call_summarize_user(
                        llm,
                        username,
                        recent_tweets,
                        retrieval_context=user_context,
                        run_id=run_id,
                    )
                    valid_user_ids = {str(t["tweet_id"]) for t in recent_tweets}
                    summary = validate_user_auto_block(summary, valid_user_ids)

                    if settings.notes.per_user_note_enabled:
                        user_note_path = settings.resolve("notes", "users", f"{username}.md")
                        user_note_live_exists = user_note_path.exists()
                        user_note_target = _prepare_target_path(user_note_path)
                        auto_body = render_user_auto_block(username, summary, recent_tweets)
                        note_res = update_note_file(
                            user_note_target,
                            note_type="user",
                            run_id=run_id,
                            now_iso=now_local,
                            auto_body=auto_body,
                            username=username,
                        )
                        _track_note(
                            note_type="user",
                            live_path=user_note_path,
                            target_path=user_note_target,
                            live_exists=user_note_live_exists,
                            note_updated=note_res.updated,
                            trigger_refs=[x_source_ref(username=username, tweet_id=t.id) for t in tweets],
                        )

                        repo.upsert_note_index(
                            NoteIndexUpsert(
                                note_path=str(user_note_path),
                                note_type="user",
                                username=username,
                                created_at=note_res.created_at,
                                updated_at=note_res.updated_at,
                                last_run_id=run_id,
                            )
                        )

                    if settings.v6.enabled:
                        new_idea_cards = propose_idea_cards(
                            run_id=run_id,
                            username=username,
                            summary=summary,
                            now_iso=now_local,
                            per_user_limit=settings.v6.idea_cards_per_user,
                            tag_aliases=tag_aliases,
                        )
                        repo.insert_idea_cards(new_idea_cards)
                        recent_idea_cards = repo.list_recent_idea_cards(days=30, limit=200, username=username)
                        idea_note_path = settings.resolve("notes", "ideas", f"{username}.md")
                        idea_note_live_exists = idea_note_path.exists()
                        idea_note_target = _prepare_target_path(idea_note_path)
                        idea_auto = render_idea_auto_block(recent_idea_cards)
                        idea_note = update_note_file(
                            idea_note_target,
                            note_type="idea",
                            run_id=run_id,
                            now_iso=now_local,
                            auto_body=idea_auto,
                            note_title=f"@{username} - Idea Cards",
                        )
                        idea_refs = [
                            ref
                            for card in new_idea_cards
                            for ref in card.get("source_refs", [])
                        ]
                        _track_note(
                            note_type="idea",
                            live_path=idea_note_path,
                            target_path=idea_note_target,
                            live_exists=idea_note_live_exists,
                            note_updated=idea_note.updated,
                            trigger_refs=idea_refs,
                        )
                        repo.upsert_note_index(
                            NoteIndexUpsert(
                                note_path=str(idea_note_path),
                                note_type="idea",
                                username=username,
                                created_at=idea_note.created_at,
                                updated_at=idea_note.updated_at,
                                last_run_id=run_id,
                            )
                        )

                    highlights_payload.append(
                        {
                            "username": username,
                            "highlights": [h.model_dump() for h in summary.highlights],
                        }
                    )
                    user_new_rows = [
                        {
                            "tweet_id": t.id,
                            "source_ref": x_source_ref(username=username, tweet_id=t.id),
                            "created_at": t.created_at_iso(),
                            "text": t.text,
                            "json": getattr(t, "raw", {}),
                        }
                        for t in tweets
                    ]
                    if settings.v7.enabled:
                        touched_entity_ids.update(
                            index_entities_from_tweets(
                                repo,
                                username=username,
                                tweets=user_new_rows,
                                now_iso=now_local,
                                min_token_len=settings.v7.min_entity_token_len,
                                alias_overrides=entity_alias_overrides,
                            )
                        )
                    digest_rows = [
                        {
                            "tweet_id": row["tweet_id"],
                            "source_ref": row["source_ref"],
                            "created_at": row["created_at"],
                            "text": row["text"],
                        }
                        for row in user_new_rows
                    ]
                    new_tweets_payload[username] = digest_rows
                    for row in digest_rows:
                        valid_digest_refs.add((username, row["tweet_id"]))
                reliability.mark_user_completed(usernames, username)
            except Exception as exc:
                reliability.mark_user_failed(usernames, username, str(exc))
                raise

        digest_context = retriever.digest_context(highlights_payload, new_tweets_payload)
        digest_block = _call_summarize_digest(
            llm,
            highlights_payload,
            new_tweets_payload,
            retrieval_context=digest_context,
            run_id=run_id,
        )
        digest_block = validate_digest_auto_block(digest_block, valid_digest_refs)
        if not digest_block.stories and not digest_block.connections:
            digest_block = DailyDigestAutoBlock()

        if settings.notes.digest_note_enabled:
            digest_path = _digest_path(settings.resolve("notes"), run_id, now_local)
            digest_live_exists = digest_path.exists()
            digest_target = _prepare_target_path(digest_path)
            digest_auto = render_digest_auto_block(digest_block)
            digest_res = update_note_file(
                digest_target,
                note_type="digest",
                run_id=run_id,
                now_iso=now_local,
                auto_body=digest_auto,
            )
            digest_refs = [x_source_ref(username=u, tweet_id=t) for (u, t) in sorted(valid_digest_refs)]
            _track_note(
                note_type="digest",
                live_path=digest_path,
                target_path=digest_target,
                live_exists=digest_live_exists,
                note_updated=digest_res.updated,
                trigger_refs=digest_refs,
            )
            repo.upsert_note_index(
                NoteIndexUpsert(
                    note_path=str(digest_path),
                    note_type="digest",
                    username=None,
                    created_at=digest_res.created_at,
                    updated_at=digest_res.updated_at,
                    last_run_id=run_id,
                )
            )

        persist_stories(
            settings,
            repo,
            digest_block,
            run_id=run_id,
            now_iso=now_local,
            report=report,
            staging_enabled=staging_enabled,
            mode="v1",
        )

        if settings.v6.enabled:
            idea_window_days = max(7, settings.v6.conflict_detection_window_days)
            recent_idea_cards = repo.list_recent_idea_cards(days=idea_window_days, limit=1500)
            conflict_cards = detect_conflict_cards(run_id=run_id, cards=recent_idea_cards, now_iso=now_local)
            repo.insert_conflict_cards(conflict_cards)
            conflict_nodes = to_conflict_nodes(
                run_id=run_id,
                now_iso=now_local,
                conflict_cards=conflict_cards,
            )
            if conflict_nodes:
                repo.upsert_conflicts(conflict_nodes)

            conflict_rows = repo.list_recent_conflict_cards(days=settings.v6.conflict_detection_window_days, limit=200)
            conflict_path = settings.resolve("notes", "conflicts", "latest.md")
            conflict_live_exists = conflict_path.exists()
            conflict_target = _prepare_target_path(conflict_path)
            conflict_auto = render_conflict_auto_block(conflict_rows)
            conflict_note = update_note_file(
                conflict_target,
                note_type="conflict",
                run_id=run_id,
                now_iso=now_local,
                auto_body=conflict_auto,
                note_title="Roberto Conflict Cards",
            )
            conflict_refs = [
                ref
                for row in conflict_rows
                for ref in row.get("source_refs", [])
            ]
            _track_note(
                note_type="conflict",
                live_path=conflict_path,
                target_path=conflict_target,
                live_exists=conflict_live_exists,
                note_updated=conflict_note.updated,
                trigger_refs=conflict_refs,
            )
            repo.upsert_note_index(
                NoteIndexUpsert(
                    note_path=str(conflict_path),
                    note_type="conflict",
                    username=None,
                    created_at=conflict_note.created_at,
                    updated_at=conflict_note.updated_at,
                    last_run_id=run_id,
                )
            )

            week_key = week_key_from_iso(now_local)
            selected_cards, connections = select_shuffle_pack(
                cards=recent_idea_cards,
                max_cards=settings.v6.shuffle_weekly_count,
                connection_count=settings.v6.shuffle_connection_count,
            )
            shuffle_path = settings.resolve("notes", "shuffles", f"{week_key}.md")
            shuffle_live_exists = shuffle_path.exists()
            shuffle_target = _prepare_target_path(shuffle_path)
            shuffle_auto = render_shuffle_auto_block(selected_cards, connections)
            shuffle_note = update_note_file(
                shuffle_target,
                note_type="shuffle",
                run_id=run_id,
                now_iso=now_local,
                auto_body=shuffle_auto,
                note_title=f"Roberto Shuffle Pack - {week_key}",
            )
            shuffle_refs = [
                ref
                for card in selected_cards
                for ref in card.get("source_refs", [])
            ]
            shuffle_refs.extend(
                ref
                for conn in connections
                for ref in conn.get("source_refs", [])
            )
            _track_note(
                note_type="shuffle",
                live_path=shuffle_path,
                target_path=shuffle_target,
                live_exists=shuffle_live_exists,
                note_updated=shuffle_note.updated,
                trigger_refs=shuffle_refs,
            )
            repo.upsert_note_index(
                NoteIndexUpsert(
                    note_path=str(shuffle_path),
                    note_type="shuffle",
                    username=None,
                    created_at=shuffle_note.created_at,
                    updated_at=shuffle_note.updated_at,
                    last_run_id=run_id,
                )
            )

        if settings.v7.enabled:
            touched_entity_ids.update(
                index_entities_from_digest(
                    repo,
                    digest_block,
                    now_iso=now_local,
                    min_token_len=settings.v7.min_entity_token_len,
                    alias_overrides=entity_alias_overrides,
                )
            )
            for entity_id in sorted(touched_entity_ids):
                entity = repo.get_entity(entity_id)
                if not entity:
                    continue
                aliases = repo.get_entity_aliases(entity_id)
                timeline_rows = repo.get_entity_timeline(
                    entity_id,
                    days=settings.v7.timeline_default_days,
                    limit=300,
                )
                entity_auto = render_entity_auto_block(
                    canonical_name=str(entity["canonical_name"]),
                    aliases=aliases,
                    timeline_rows=timeline_rows,
                    days=settings.v7.timeline_default_days,
                )
                entity_path = settings.resolve("notes", "entities", f"{entity_id}.md")
                entity_live_exists = entity_path.exists()
                entity_target = _prepare_target_path(entity_path)
                entity_note = update_note_file(
                    entity_target,
                    note_type="entity",
                    run_id=run_id,
                    now_iso=now_local,
                    auto_body=entity_auto,
                    note_title=f"Entity - {entity['canonical_name']}",
                    entity_id=entity_id,
                    entity_name=str(entity["canonical_name"]),
                )
                entity_refs = [
                    {"username": str(row.get("username")), "tweet_id": str(row.get("ref_id"))}
                    for row in timeline_rows
                    if row.get("ref_type") == "tweet"
                ]
                _track_note(
                    note_type="entity",
                    live_path=entity_path,
                    target_path=entity_target,
                    live_exists=entity_live_exists,
                    note_updated=entity_note.updated,
                    trigger_refs=entity_refs,
                )
                repo.upsert_note_index(
                    NoteIndexUpsert(
                        note_path=str(entity_path),
                        note_type="entity",
                        username=None,
                        created_at=entity_note.created_at,
                        updated_at=entity_note.updated_at,
                        last_run_id=run_id,
                    )
                )

        if settings.v18.enabled:
            briefing = build_daily_briefing(
                repo,
                digest_block,
                run_id=run_id,
                now_iso=now_local,
                top_story_deltas=settings.v18.top_story_deltas,
                top_connections=settings.v18.top_connections,
                top_ideas=settings.v18.top_ideas,
            )
            briefing_path = settings.resolve("notes", "briefings", f"{briefing.brief_date}.md")
            briefing_live_exists = briefing_path.exists()
            briefing_target = _prepare_target_path(briefing_path)
            briefing_auto = render_briefing(briefing.summary, mode=settings.v18.default_mode)
            briefing_note = update_note_file(
                briefing_target,
                note_type="briefing",
                run_id=run_id,
                now_iso=now_local,
                auto_body=briefing_auto,
                note_title=f"Roberto Daily Briefing - {briefing.brief_date}",
            )
            _track_note(
                note_type="briefing",
                live_path=briefing_path,
                target_path=briefing_target,
                live_exists=briefing_live_exists,
                note_updated=briefing_note.updated,
                trigger_refs=briefing.refs,
            )
            repo.upsert_note_index(
                NoteIndexUpsert(
                    note_path=str(briefing_path),
                    note_type="briefing",
                    username=None,
                    created_at=briefing_note.created_at,
                    updated_at=briefing_note.updated_at,
                    last_run_id=run_id,
                )
            )
            repo.upsert_briefing(
                brief_id=briefing.brief_id,
                run_id=run_id,
                brief_date=briefing.brief_date,
                note_path=str(briefing_path),
                summary=briefing.summary,
                created_at=briefing_note.created_at,
                updated_at=briefing_note.updated_at,
            )
            repo.replace_briefing_items(
                brief_id=briefing.brief_id,
                run_id=run_id,
                items=briefing.item_rows,
                created_at=now_local,
            )

        if settings.v19.enabled:
            report.greene_stats = run_greene_cycle(
                settings,
                repo,
                run_id=run_id,
                now_iso=now_local,
            )
            if settings.v21.enabled:
                report.greene_stats.update(
                    run_chapter_argument_gap_cycle(
                        settings,
                        repo,
                        run_id=run_id,
                        now_iso=now_local,
                    )
                )

        if settings.v17.eval.enabled:
            eval_result = run_eval(settings)
            report.eval_gate_passed = bool(eval_result.passed)
            report.eval_gate_metrics = dict(eval_result.metrics)
            report.eval_gate_failures = list(eval_result.failures or [])

        report.finished_at = utc_now_iso()
        rebuild_search_index(settings, repo)
        repo.finish_run(run_id, report.finished_at, report.to_dict())
        export_path = settings.resolve("data", "exports", f"run_{run_id}.json")
        report.write_json(export_path)
        reliability.finish(usernames, success=True)
        return report
    except Exception:
        reliability.finish(usernames, success=False)
        raise
