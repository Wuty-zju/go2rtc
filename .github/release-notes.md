Maintenance release for native Xiaomi dual-lens streams.

- Give each talkback client on the second lens its own input at the shared speaker. Disconnecting one client releases its return track and preserves other active inputs across camera reconnects.
- Avoid a duplicate speaker-start command when speech resumes before the idle callback runs. Partial audio from the previous speaker is discarded at handoff.
- Finish an already-closed output immediately if its writer attaches late. This covers clients that disconnect before MP4 streaming starts.

The existing stream URLs and audio configuration continue to work.

Container tags are `dev`, `dev-hardware` and `dev-rockchip`.
