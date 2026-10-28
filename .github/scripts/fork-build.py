"""Fork-only distribution helpers. No camera credentials or workspace inputs."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import zipfile


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


def output(**values):
    with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
        for key, value in values.items():
            stream.write(key + '=' + (value if isinstance(value, str) else json.dumps(value)) + '\n')


def plan():
    event = os.environ['EVENT_NAME']
    ref_type, ref_name = os.environ['REF_TYPE'], os.environ['REF_NAME']
    tagged = ref_type == 'tag'
    distribution = json.loads(Path('.github/distribution.json').read_text())
    versions, baselines = distribution['versions'], distribution['upstream']
    if set(versions) != {'master', 'dev'} or set(baselines) != {'master', 'dev'}:
        raise SystemExit('Both release channels require explicit versions and upstream baselines')
    for channel, version in versions.items():
        if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:\.[0-9]+)?(?:_dev)?', version):
            raise SystemExit('Invalid distribution version')
        if version.endswith('_dev') != (channel == 'dev'):
            raise SystemExit('Stable/dev version channel mismatch')
    if tagged:
        matches = [channel for channel, version in versions.items() if ref_name == 'v' + version]
        if len(matches) != 1:
            raise SystemExit('Tag and configured distribution version differ')
        channel = matches[0]
        if subprocess.run(['git', 'merge-base', '--is-ancestor', 'HEAD', 'origin/' + channel]).returncode:
            raise SystemExit('Release tag is not contained in its master/dev channel')
    else:
        channel = os.environ.get('BASE_REF', '') if event == 'pull_request' else ref_name
        if channel not in versions:
            raise SystemExit('Build and publication are limited to master/dev')
    baseline = baselines[channel]
    if not re.fullmatch(r'[0-9a-f]{40}', baseline):
        raise SystemExit('Upstream baseline must be an immutable commit SHA')
    if subprocess.run(['git', 'merge-base', '--is-ancestor', baseline, 'HEAD']).returncode:
        raise SystemExit('Selected upstream baseline is not in the source history')
    release = tagged and event == 'push'
    publish = release or (event == 'push' and not tagged) or (
        event == 'workflow_dispatch' and not tagged and os.environ.get('PUBLISH_INPUT') == 'true')
    source = dict(channel=channel, sha=git('rev-parse', 'HEAD'),
                  tag=('dev' if channel == 'dev' else versions[channel]) if tagged else channel,
                  version=versions[channel] if tagged else '',
                  baseline_name=channel, baseline_sha=baseline)
    output(sources=[source], **source, publish=str(publish).lower(), release=str(release).lower())
    print(json.dumps(source, indent=2))


def ldflags(version):
    return '-s -w' + (' -X github.com/AlexxIT/go2rtc/internal/app.DistributionVersion=' + version if version else '')


def binaries():
    targets = [('win64','windows','amd64',''), ('win32','windows','386',''),
               ('win_arm64','windows','arm64',''), ('linux_amd64','linux','amd64',''),
               ('linux_i386','linux','386',''), ('linux_arm64','linux','arm64',''),
               ('linux_arm','linux','arm','7'), ('linux_armv6','linux','arm','6'),
               ('linux_mipsel','linux','mipsle',''), ('mac_amd64','darwin','amd64',''),
               ('mac_arm64','darwin','arm64',''), ('freebsd_amd64','freebsd','amd64',''),
               ('freebsd_arm64','freebsd','arm64','')]
    dest = Path(os.environ['RUNNER_TEMP']) / 'dist'
    dest.mkdir(exist_ok=True)
    records = []
    for name, goos, arch, arm in targets:
        env = dict(os.environ, GOOS=goos, GOARCH=arch, CGO_ENABLED='0')
        env.pop('GOARM', None)
        if arm:
            env['GOARM'] = arm
        base = 'go2rtc_' + name
        binary = dest / (base + ('.exe' if goos == 'windows' else ''))
        subprocess.run(['go', 'build', '-mod=readonly', '-trimpath', '-ldflags=' + ldflags(os.environ['VERSION']), '-o', str(binary), '.'], env=env, check=True)
        info = subprocess.check_output(['go','version','-m',str(binary)], text=True)
        required = ['vcs.revision=' + os.environ['SOURCE_SHA'], 'vcs.modified=false', 'GOOS=' + goos, 'GOARCH=' + arch, 'CGO_ENABLED=0']
        if arm:
            required.append('GOARM=' + arm)
        if not all(value in info for value in required):
            raise SystemExit('Binary identity mismatch: ' + base)
        record = dict(target=base, goos=goos, goarch=arch, goarm=arm, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
        if goos != 'linux':
            archive = dest / (base + '.zip')
            with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zipped:
                zipped.write(binary, 'go2rtc.exe' if goos == 'windows' else 'go2rtc')
            binary.unlink()
            record['asset'] = archive.name
        else:
            record['asset'] = binary.name
        records.append(record)
    (dest / 'build.json').write_text(json.dumps(dict(source_sha=os.environ['SOURCE_SHA'], distribution=os.environ['VERSION'], go=subprocess.check_output(['go','version'],text=True).strip(), binaries=records), indent=2) + '\n')
    (dest / 'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest() + '  ' + p.name + '\n' for p in sorted(dest.iterdir()) if p.name != 'SHA256SUMS'))


def recipe():
    source = Path(os.environ['DOCKERFILE'])
    text = source.read_text()
    marker = 'CGO_ENABLED=0 go build'
    expected_arch_args = 2 if source.name == 'Dockerfile' else 1
    if text.count(marker) != 1 or text.count('ARG TARGETARCH') != expected_arch_args:
        raise SystemExit('Upstream recipe changed; review target mapping')
    text = text.replace('ARG TARGETARCH', 'ARG TARGETARCH\nARG TARGETVARIANT', 1)
    text = text.replace(marker, 'if [ "${TARGETARCH}" = "arm" ]; then export GOARM="${TARGETVARIANT#v}"; fi; ' + marker)
    if os.environ['VERSION']:
        if text.count('-ldflags "-s -w"') != 1:
            raise SystemExit('Unexpected linker flags in recipe')
        text = text.replace('-ldflags "-s -w"', '-ldflags "' + ldflags(os.environ['VERSION']) + '"')
    dest = Path(os.environ['RUNNER_TEMP']) / 'Dockerfile.fork'
    dest.write_text(text)
    output(file=str(dest))
    print('Recipe SHA256:', hashlib.sha256(dest.read_bytes()).hexdigest())


if __name__ == '__main__':
    {'plan': plan, 'binaries': binaries, 'recipe': recipe}[sys.argv[1]]()
