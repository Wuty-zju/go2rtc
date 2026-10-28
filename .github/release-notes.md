Based on upstream go2rtc 1.9.15 with Xiaomi dual-lens support (082ac1, explicit channel=all). One device session supplies two video tracks and a shared microphone/speaker. The first active talkback input owns the speaker; another input can take over after one second without accepted audio.

Includes RTSP packet/keyframe handling, H.265 snapshots and stream reconnect fixes. Shared talkback has simulated and loopback coverage; physical HomeKit talkback and long-term stability still need validation.

Images: `ghcr.io/wuty-zju/go2rtc:dev` for development; `:latest` or `:1.9.15.1` for the stable fork. Upstream hardware/rockchip variants use `-hardware` and `-rockchip`. Development images have no numbered tags. This is a fork build, not an AlexxIT release.
