#!/bin/bash
# Force-clean container-backend leftovers on this host (after a crashed/killed node server).
for root in /run/aenv-ct-*/runc; do
  [ -d "$root" ] || continue
  for c in $(sudo runc --root $root list -q 2>/dev/null); do sudo runc --root $root resume $c 2>/dev/null; sudo runc --root $root delete --force $c; done
done
for m in $(findmnt -rn -o TARGET | grep -E '/home/aenv-ct[^ ]*/container/work/.*/rootfs$'); do sudo umount -l "$m"; done
for n in $(ip netns list | awk '/^aenvct-/{print $1}'); do sudo ip netns del $n; done
echo cleaned
