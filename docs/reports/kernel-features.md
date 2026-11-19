# Kernel feature report — development host

- **Date:** 2026-11-19
- **Phase:** P0, feature confirmation ([IMPLEMENTATION.md](../../specifications/IMPLEMENTATION.md) §4, P0)
- **Purpose:** check the kernel features that the sandbox of [SPECIFICATIONS.md](../../specifications/SPECIFICATIONS.md) §6.4 needs, and the fallbacks of PM-ADP-005.

## Host

| Item | Value |
|---|---|
| Kernel | `6.18.33.2-microsoft-standard-WSL2` |
| Distribution | Ubuntu 26.04 LTS, under WSL2 |
| CPU | 16 logical CPUs |
| Memory | 15,991 MiB |
| Root filesystem | ext4, 712 GiB free |
| Architecture | Not checked in this report (`uname -m` to add) |

This host is a development machine. It is **not a qualified hardware profile** ([GW] NFR-PERF, §6.1): performance and HA results are not valid on it.

## Results

| Feature | Needed for | Observed | Result |
|---|---|---|---|
| cgroup v2 | Per-adapter cgroups (§6.4) | `cgroup2fs` on `/sys/fs/cgroup`, mounted with `nsdelegate`; controllers `cpu io memory pids` and others | Available |
| cgroup delegation to the user | Core creates child cgroups for adapters | `systemd-run --user --scope -p Delegate=yes` runs in `user.slice/user-1000.slice/user@1000.service/…`, a delegated subtree | Available through `systemd --user`. The bare shell cgroup (`/init.scope`, owned by root) is not delegated |
| Landlock | Filesystem and TCP restriction (§6.4) | Kernel config `CONFIG_SECURITY_LANDLOCK=y`. ABI probe through the syscall: **ABI 7** | Available. ABI ≥ 4 gives TCP bind and connect restrictions, so the nftables fallback is not needed here |
| Landlock in `securityfs` | Listing active LSMs | `/sys/kernel/security` is not mounted; `/sys/kernel/security/lsm` is empty | Not usable as a check. Use the ABI probe |
| seccomp filter | Per-adapter allow-list (§6.4) | `CONFIG_SECCOMP_FILTER=y`; the shell runs in seccomp mode 2 | Available |
| User namespaces | Unprivileged sandbox (§6.4), Podman rootless | `unshare -Ur` works; `max_user_namespaces` = 63,923 | Available |
| Subordinate uid/gid | Per-instance uids | `aklira:100000:65536` in `/etc/subuid` and `/etc/subgid` | Available for development. See the note below |
| nftables | Network rules of the cell (fallback to Landlock) | `nft` present; needs root; no passwordless `sudo` | **Not testable here.** Not needed on this host, because Landlock ABI 7 covers it |
| Rootless Podman | Cell container (§18.2) | `rootless=true`, cgroups v2, runtime `crun` | Available |
| Time sync | NFR-TIME-001, ACC-006 | `chronyc` present; `ptp4l` and `phc2sys` absent | NTP only. PTP tested on hardware in P7 |
| Protobuf lint | `buf lint`, `buf breaking` (PM-PIV-002) | `buf` absent | To install in P1 |

## Notes

1. **Uid range differs between development and production.** SPECIFICATIONS.md §6.4 allocates uids from `20000–20999`. In a user namespace, the only range available here is `100000:65536`. The specification must say which range applies in development and which in production. This is a follow-up on the specification.
2. **Cgroup delegation is per-scope.** The core must run inside a delegated scope. The Quadlet unit uses `Delegate=yes` in production (§18.2). In development, run the core under `systemd-run --user --scope -p Delegate=yes`.
3. **PM-ADP-005 applies.** Production refuses to start an adapter if a required feature is missing. On this host every required feature is present except nftables, which is not needed.

## Conclusion

Every feature needed by the sandbox is available for development on this host, except that nftables cannot be tested without root. The sandbox of P1 can be built and tested here. Production behaviour (rootful container, real hardware, watchdog) is still to be confirmed on the qualified hardware (see [bench.md](../bench.md)).
