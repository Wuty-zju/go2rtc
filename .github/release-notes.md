Maintenance release of the Wuty-zju fork, keeping the existing native Xiaomi configuration and media ownership model.

- Reuse the activity-based speaker lifecycle for both single- and dual-lens producers. Preview subscriptions no longer start the single-lens speaker; accepted talkback audio starts it, and idle activity releases it.
- Reject truncated CS2 media headers before slicing and synchronize command acknowledgement callbacks. These are small transport fixes with independent failure regressions.
- Preserve one physical MISS connection for both lens outlets, the shared microphone and speaker, and lossless optional `ptime=20` framing with the standard 48 kHz Opus RTP clock.

The existing stream names and URLs remain compatible. Receiver-side HKSV3 initialization sampling is maintained separately in the local camera.ui adapter. Automated checks cover the changed boundaries; physical audio quality and iCloud clip availability remain separate acceptance items.

Container: `ghcr.io/wuty-zju/go2rtc:1.9.15.7` (`latest` and `master` aliases; hardware and Rockchip variants keep the corresponding suffixes).
