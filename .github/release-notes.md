Based on upstream go2rtc 1.9.15, with the reviewed Xiaomi changes shared by the stable and development builds.

- Reuse both 082ac1 lenses through one physical camera connection. Keep the ordinary primary Xiaomi URL; the second channel adds `reuse=<primary stream>`.
- Share the microphone and one speaker between both outlets. The first active talkback input owns the speaker until one second without accepted audio; inputs are not mixed.
- Add opt-in `ptime=20` for lossless splitting of supported Opus packets, preserving coded frames and the 48 kHz RTP clock. Defaults and speaker codecs stay unchanged.
- Improve subscription cleanup, reconnect media identity, RTSP transport ownership and cancelled snapshots. Keep the original architecture; no global camera pool or additional forward-media queue.

Automated checks and prior bounded camera evidence cover the scoped changes. Complete Apple Home talkback quality, competing-input handover and long-duration stability remain unqualified. Known camera.ui receiver-side issues are not changed by this fork. This is not an AlexxIT release.

Containers: `ghcr.io/wuty-zju/go2rtc:dev` for development; `:latest` or `:1.9.15.2` for the stable fork. Hardware/rockchip variants retain their upstream suffixes. Development containers use a rolling `dev` tag, without a numbered `_dev` image tag.

This replaces the earlier fork build carrying the same version at the user's explicit request. Identify this rebuild by its full source revision and SHA256SUMS.
