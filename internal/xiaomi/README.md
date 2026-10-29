# Xiaomi Mi Home

[`new in v1.9.13`](https://github.com/AlexxIT/go2rtc/releases/tag/v1.9.13)

This source allows you to view cameras from the [Xiaomi Mi Home](https://home.mi.com/) ecosystem.

Since 2020, Xiaomi has introduced a unified protocol for cameras called `miss`. I think it means **Mi Secure Streaming**. Until this point, the camera protocols were in chaos. Almost every model had different authorization, encryption, command lists, and media packet formats.

go2rtc supports two formats: `xiaomi/mess` and `xiaomi/legacy`.
And multiple P2P protocols: `cs2+udp`, `cs2+tcp`, several versions of `tutk+udp`.

Almost all cameras in the `xiaomi/mess` format and the `cs2` protocol work well.
Older `xiaomi/legacy` format cameras may have support issues.
The `tutk` protocol is the worst thing that's ever happened to the P2P world. It works terribly.

**Important:**

1. **Not all cameras are supported**. The list of supported cameras is collected in [this issue](https://github.com/AlexxIT/go2rtc/issues/1982).
2. Each time you connect to the camera, you need Internet access to obtain encryption keys.
3. Connection to the camera is local only.

**Features:**

- Multiple Xiaomi accounts supported
- Cameras from multiple regions are supported for a single account
- Two-way audio is supported
- Cameras with multiple lenses are supported

## Setup

1. Go to go2rtc WebUI > Add > Xiaomi > Login with username and password
2. Receive verification code by email or phone if required.
3. Complete the captcha if required.
4. If everything is OK, your account will be added, and you can load cameras from it.

**Example**

```yaml
xiaomi:
  1234567890: V1:***

streams:
  xiaomi1: xiaomi://1234567890:cn@192.168.1.123?did=9876543210&model=isa.camera.hlc7
```

## Configuration

Quality in the `miss` protocol is specified by a number from 0 to 5. Usually 0 means auto, 1 - sd, 2 - hd.
Go2rtc by default sets quality to 2. But some new cameras have HD quality at number 3.
Old cameras may have broken codec settings at number 3, so this number should not be set for all cameras.

You can change camera quality: `subtype=hd/sd/auto/0-5`.

```yaml
streams:
  xiaomi1: xiaomi://***&subtype=sd
```

You can use a second channel for dual cameras: `channel=2`.

```yaml
streams:
  xiaomi1: xiaomi://***&channel=2
```

## Shared dual camera

For the verified `xiaomi.camera.082ac1`, declare the ordinary primary URL and
reference its stream from the second channel with `reuse`:

```yaml
streams:
  primary: xiaomi://USER:cn@CAMERA?did=DID&model=xiaomi.camera.082ac1&subtype=3
  secondary: xiaomi://USER:cn@CAMERA?did=DID&model=xiaomi.camera.082ac1&subtype=3&channel=2&reuse=primary
```

This opens one device connection with two video tracks, a shared microphone and
one speaker. Either outlet can use the speaker; the first active input owns it
until it sends no audio packets for one second. Concurrent inputs are not mixed.

The secondary URL must identify the same account, region and device. Omitted
options inherit the primary's settings; explicit options must match. No local
RTSP service is required for reuse: the secondary consumes the primary Stream
through the existing media graph. Existing FFmpeg sources on the primary remain
available to the secondary.

Declare both streams before startup. Changing their reuse relationship through
the API requires restarting go2rtc; an active single-track device connection is
not silently upgraded. Sources without a reuse declaration retain their normal
single-channel behavior. Other dual camera models are not yet verified.

Protocol media IDs `0` and `1` distinguish video channels; they do not name the
physical lenses. The earlier `channel=all` and explicit RTSP selection remain
available for compatibility with existing configurations.
