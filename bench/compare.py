#!/usr/bin/env python3
# compare.py <tsvA> <tsvB> [labelA labelB]: per-case min-of-runs, ratio, flags
import sys, collections
def load(p):
    d = collections.defaultdict(list)
    for line in open(p):
        if line.startswith('#') or not line.strip(): continue
        f = line.rstrip('\n').split('\t')
        # label case bytes ms alloc mibs
        d[f[1]].append((float(f[3]), int(f[4]), float(f[2])))
    return d
A, B = load(sys.argv[1]), load(sys.argv[2])
la, lb = (sys.argv[3], sys.argv[4]) if len(sys.argv) > 4 else ('A', 'B')
print(f"{'case':42s} {'runs':>4s} {la+' ms':>11s} {lb+' ms':>11s} {'B/A':>7s} {'MiB/s '+lb:>11s}  {'alloc%':>8s}")
worse, better = [], []
for c in sorted(set(A) | set(B)):
    if c not in A or c not in B:
        print(f"{c:42s}  only in {'A' if c in A else 'B'}"); continue
    a = min(x[0] for x in A[c]); b = min(x[0] for x in B[c])
    aa = min(x[1] for x in A[c]); ab_ = min(x[1] for x in B[c])
    by = B[c][0][2]
    r = b / a if a > 0 else float('nan')
    flag = '  <<< slower' if r > 1.05 else ('  >>> faster' if r < 0.95 else '')
    (worse if r > 1.05 else better if r < 0.95 else []).append(c)
    dalloc = (ab_ - aa) / max(aa, 1) * 100
    print(f"{c:42s} {len(A[c]):>2d}/{len(B[c]):<2d} {a:11.3f} {b:11.3f} {r:7.3f} {by/2**20/(b/1000):11.0f}  {dalloc:+7.1f}%{flag}")
print(f"\nfaster: {len(better)}  slower: {len(worse)}")
