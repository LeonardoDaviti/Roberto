from __future__ import annotations

from pathlib import Path

from roberto_app.llm.schemas import DailyDigestAutoBlock
from roberto_app.notesys.renderer import render_digest_auto_block, render_user_auto_block
from roberto_app.notesys.updater import update_note_file
from roberto_app.pipeline.common import local_now_iso, newest_tweet_id, read_following, run_id_now, utc_now_iso
from roberto_app.pipeline.report import RunReport
from roberto_app.storage.repo import NoteIndexUpsert, StorageRepo
from roberto_app.x_api.client import XClient


def _digest_path(notes_dir: Path, run_id: str, now_local_iso: str) -> Path:
    date_part = now_local_iso[:10]
    time_part = now_local_iso[11:19].replace(":", "")
    return notes_dir / "digests" / f"{date_part}__run-{time_part}.md"


def run_v1(settings, repo: StorageRepo, x_client: XClient, llm) -> RunReport:
    usernames = read_following(settings.resolve("config", "following.txt"))
    run_id = run_id_now()
    started_at = utc_now_iso()
    now_local = local_now_iso(settings.notes.note_timezone)

    report = RunReport(run_id=run_id, mode="v1", started_at=started_at)
    repo.create_run(run_id, "v1", started_at)

    highlights_payload: list[dict] = []
    new_tweets_payload: dict[str, list[dict]] = {}

    for username in usernames:
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
        summary = llm.summarize_user(username, recent_tweets)

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
        new_tweets_payload[username] = [
            {
                "tweet_id": t.id,
                "created_at": t.created_at_iso(),
                "text": t.text,
            }
            for t in tweets
        ]

    digest_block = llm.summarize_digest(highlights_payload, new_tweets_payload)
    if not digest_block.stories and not digest_block.connections:
        digest_block = DailyDigestAutoBlock()

    if settings.notes.digest_note_enabled:
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

    report.finished_at = utc_now_iso()
    repo.finish_run(run_id, report.finished_at, report.to_dict())
    export_path = settings.resolve("data", "exports", f"run_{run_id}.json")
    report.write_json(export_path)
    return report
