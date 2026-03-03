from __future__ import annotations

import math
from pathlib import Path

from roberto_app.llm.retrieval import RetrievalContextBuilder
from roberto_app.llm.schemas import DailyDigestAutoBlock
from roberto_app.llm.validation import validate_digest_auto_block, validate_user_auto_block
from roberto_app.notesys.renderer import render_digest_auto_block, render_user_auto_block
from roberto_app.notesys.updater import update_note_file
from roberto_app.pipeline.common import local_now_iso, newest_tweet_id, read_following, run_id_now, utc_now_iso
from roberto_app.pipeline.report import RunReport
from roberto_app.pipeline.story_memory import persist_stories
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo
from roberto_app.x_api.client import XClient


def _digest_path(notes_dir: Path, run_id: str, now_local_iso: str) -> Path:
    date_part = now_local_iso[:10]
    time_part = now_local_iso[11:19].replace(":", "")
    return notes_dir / "digests" / f"{date_part}__run-{time_part}.md"


def run_v2(settings, repo: StorageRepo, x_client: XClient | None, llm, *, from_db_only: bool = False) -> RunReport:
    usernames = read_following(settings.resolve("config", "following.txt"))
    run_id = run_id_now()
    started_at = utc_now_iso()
    now_local = local_now_iso(settings.notes.note_timezone)

    report = RunReport(run_id=run_id, mode="v2", started_at=started_at)
    repo.create_run(run_id, "v2", started_at)
    retriever = RetrievalContextBuilder(repo, settings.v4.retrieval)

    highlights_payload: list[dict] = []
    new_tweets_payload: dict[str, list[dict]] = {}
    valid_digest_refs: set[tuple[str, str]] = set()

    page_cap = max(1, math.ceil(settings.pipeline.v2.max_new_tweets_per_user / settings.x.max_results))

    for username in usernames:
        user_row = repo.get_user(username)
        if not user_row:
            if from_db_only:
                repo.upsert_user(username, f"local:{username}", username)
                user_row = repo.get_user(username)
            else:
                if x_client is None:
                    raise RuntimeError("X client is required when from_db_only is disabled")
                looked_up = x_client.lookup_user(username)
                repo.upsert_user(username, looked_up.id, looked_up.name)
                user_row = repo.get_user(username)

        if user_row and not user_row.get("user_id") and from_db_only:
            repo.upsert_user(username, f"local:{username}", user_row.get("display_name") or username)
            user_row = repo.get_user(username)

        if not user_row:
            report.per_user_new_tweets[username] = 0
            continue

        user_id = str(user_row.get("user_id") or f"local:{username}")
        last_seen = user_row.get("last_seen_tweet_id")

        if from_db_only:
            new_rows = repo.get_tweets_since_id(
                username,
                since_id=last_seen,
                limit=settings.pipeline.v2.max_new_tweets_per_user,
            )
            inserted_count = len(new_rows)
            report.per_user_new_tweets[username] = inserted_count
            newest = newest_tweet_id([str(t["tweet_id"]) for t in new_rows])
            repo.update_user_state(username, newest or last_seen, utc_now_iso())
            if inserted_count == 0:
                continue
        else:
            if x_client is None:
                raise RuntimeError("X client is required when from_db_only is disabled")
            tweets = x_client.fetch_user_tweets(
                user_id,
                since_id=last_seen,
                max_results=settings.x.max_results,
                exclude=settings.x.exclude,
                tweet_fields=settings.x.tweet_fields,
                max_pages=page_cap,
            )
            inserted_count = repo.insert_tweets(username, tweets)
            report.per_user_new_tweets[username] = inserted_count

            newest = newest_tweet_id([t.id for t in tweets])
            repo.update_user_state(username, newest or last_seen, utc_now_iso())

            if inserted_count == 0:
                continue

            new_rows = [
                {
                    "tweet_id": t.id,
                    "created_at": t.created_at_iso(),
                    "text": t.text,
                }
                for t in tweets
            ]

        recent_tweets = repo.get_recent_tweets(username, limit=settings.pipeline.v1.backfill_count)
        if from_db_only:
            focus_ids = {str(t["tweet_id"]) for t in new_rows}
        else:
            focus_ids = {str(t.id) for t in tweets}
        user_context = retriever.user_context(username, recent_tweets, focus_tweet_ids=focus_ids)
        summary = llm.summarize_user(username, recent_tweets, retrieval_context=user_context)
        valid_user_ids = {str(t["tweet_id"]) for t in recent_tweets}
        summary = validate_user_auto_block(summary, valid_user_ids)

        if settings.notes.per_user_note_enabled:
            user_note_path = settings.resolve("notes", "users", f"{username}.md")
            auto_body = render_user_auto_block(username, summary, recent_tweets)
            note_res = update_note_file(
                user_note_path,
                note_type="user",
                run_id=run_id,
                now_iso=now_local,
                auto_body=auto_body,
                username=username,
            )
            if note_res.created:
                report.created_notes.append(str(user_note_path))
            elif note_res.updated:
                report.updated_notes.append(str(user_note_path))

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

        highlights_payload.append(
            {
                "username": username,
                "highlights": [h.model_dump() for h in summary.highlights],
            }
        )
        if from_db_only:
            user_new_rows = [
                {
                    "tweet_id": t["tweet_id"],
                    "created_at": t["created_at"],
                    "text": t["text"],
                }
                for t in new_rows
            ]
            new_tweets_payload[username] = user_new_rows
        else:
            user_new_rows = new_rows
            new_tweets_payload[username] = user_new_rows
        for row in user_new_rows:
            valid_digest_refs.add((username, row["tweet_id"]))

    if settings.pipeline.v2.create_digest_each_run:
        digest_context = retriever.digest_context(highlights_payload, new_tweets_payload)
        digest_block = llm.summarize_digest(
            highlights_payload,
            new_tweets_by_user=new_tweets_payload,
            retrieval_context=digest_context,
        )
        digest_block = validate_digest_auto_block(digest_block, valid_digest_refs)
        if not digest_block.stories and not digest_block.connections:
            digest_block = DailyDigestAutoBlock()

        digest_path = _digest_path(settings.resolve("notes"), run_id, now_local)
        digest_auto = render_digest_auto_block(digest_block)
        digest_res = update_note_file(
            digest_path,
            note_type="digest",
            run_id=run_id,
            now_iso=now_local,
            auto_body=digest_auto,
        )
        report.created_notes.append(str(digest_path))
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
        )

    report.finished_at = utc_now_iso()
    repo.finish_run(run_id, report.finished_at, report.to_dict())
    export_path = settings.resolve("data", "exports", f"run_{run_id}.json")
    report.write_json(export_path)
    return report
