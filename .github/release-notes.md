Development build based on upstream go2rtc dev, with the existing Xiaomi dual-lens and lossless Opus changes.

- Release the shared Xiaomi speaker after one second without accepted talkback audio, while video and microphone streaming continue. The next talkback input starts it again. Both lens outlets share one speaker; concurrent inputs are not mixed.
- Avoid an unnecessary header allocation when splitting supported Opus packets with `ptime=20`. Preserve the encoded frames and the standard 48 kHz RTP clock; no FFmpeg or audio re-encoding is added.
- Keep existing Xiaomi URLs: the secondary lens uses `channel=2&reuse=<primary stream>`. No configuration migration is required from the previous fork dev build.

Scoped automated regressions and the existing local evidence cover these changes. Device-specific speaker release, Mi Home coexistence and complete paired HomeKit audio quality remain under evaluation. Receiver-side camera.ui issues are unchanged. This is a Wuty-zju fork prerelease, not an AlexxIT release.

Container: `docker pull ghcr.io/wuty-zju/go2rtc:dev`. Hardware and Rockchip variants use `:dev-hardware` and `:dev-rockchip`; development images have no numbered `_dev` tag. The stable `latest` image is unchanged.
