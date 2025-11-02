import os
import mmap
import math
import multiprocessing as mp
from collections import defaultdict

def str_parse_x10(b: bytes, start: int, end: int) -> int:
    # b[start:end] 는 예: b"-12.3" 또는 b"7.0" 또는 b"23" (1brc는 소수점 보장되지만 안전하게)
    neg = False
    i = start
    if b[i] == 45:  # '-'
        neg = True
        i += 1
    val = 0
    frac = 0
    frac_len = 0
    while i < end:
        c = b[i]
        if c == 46:  # '.'
            i += 1
            # 소수 첫자리만 사용(×10)
            if i < end and 48 <= b[i] <= 57:
                frac = b[i] - 48
                frac_len = 1
                i += 1
            # 남는 자리(있다면) 
            while i < end and 48 <= b[i] <= 57:
                i += 1
            break
        elif 48 <= c <= 57:
            val = val * 10 + (c - 48)
            i += 1
        else:
            # 이상한 문자
            break
    out = val * 10 + (frac if frac_len else 0)
    return -out if neg else out

# --------- 워커: 바이트 청크 집계 ----------
def process_chunk(args):
    path, start, end = args
    result = {}  # {station_bytes: (cnt, sum_i10, min_i10, max_i10)}
    with open(path, 'rb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
        i = start
        # 라인 단위: station;temp\n
        while i < end:
            # station 시작
            station_start = i
            # 세미콜론 찾기
            semi = mm.find(b';', i, end)
            if semi == -1:
                break  # 남은 라인 없음
            # 온도 시작은 semi+1
            temp_start = semi + 1
            # 개행 찾기
            nl = mm.find(b'\n', temp_start, end)
            if nl == -1:
                nl = end
            # station 슬라이스
            station = mm[station_start:semi]
            # temp 슬라이스
            t = str_parse_x10(mm, temp_start, nl)

            # 최신화 (로컬 dict에 바로 튜플 재구성보다 setdefault+언패킹이 빠른 경우가 많음)
            if station in result:
                cnt, s, mn, mx = result[station]
                cnt += 1
                s += t
                if t < mn: mn = t
                if t > mx: mx = t
                result[station] = (cnt, s, mn, mx)
            else:
                result[station] = (1, t, t, t)

            i = nl + 1
    return result

def make_chunks(path, target_chunks):
    size = os.path.getsize(path)
    if size == 0:
        return []

    with open(path, 'rb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
        # 기본 등분 오프셋
        raw = [0]
        for k in range(1, target_chunks):
            raw.append((size * k) // target_chunks)
        raw.append(size)

        chunks = []
        prev = 0
        for k in range(1, len(raw)):
            start = raw[k-1]
            end = raw[k]

            # start는 이전 청크 끝 이후 첫 라인 시작으로 정렬
            if start != 0:
                # 이전 개행 다음으로
                s = mm.find(b'\n', start, min(start+1024, size))
                start = (s + 1) if s != -1 else start
            # end는 라인 끝(개행)까지 포함
            if end != size:
                e = mm.rfind(b'\n', max(end-1024, 0), end)
                end = (e + 1) if e != -1 else end

            # 겹치기 방지 및 비어있는 청크 제거
            start = max(start, prev)
            if end > start:
                chunks.append((start, end))
                prev = end

        return chunks

def aggregate_file(path: str, n_chunks: int | None = None, n_procs: int | None = None):
    size = os.path.getsize(path)
    if size == 0:
        return {}

    # 청킹 수 결정 (파일 크기/코어 수를 수정)
    if n_chunks is None:
        cpu = os.cpu_count() or 4
        n_chunks = max(cpu, 1) * 4  # 코어 x4 정도 추천이라고 함

    chunks = make_chunks(path, n_chunks)
    if not chunks:
        return {}

    # 프로세스 수 = min(코어 수, 청크 수)  (프로세스 > 청크 -> None)
    if n_procs is None:
        n_procs = min(len(chunks), os.cpu_count() or 4)

    tasks = [(path, s, e) for (s, e) in chunks]

    with mp.Pool(processes=n_procs, maxtasksperchild=64) as pool:
        partials = pool.map(process_chunk, tasks)

    # Reduce: dict merge
    final = {}  # {station_bytes: (cnt, sum_i10, min_i10, max_i10)}
    for d in partials:
        for k, v in d.items():
            if k in final:
                c1, s1, mn1, mx1 = final[k]
                c2, s2, mn2, mx2 = v
                c = c1 + c2
                s = s1 + s2
                mn = mn1 if mn1 < mn2 else mn2
                mx = mx1 if mx1 > mx2 else mx2
                final[k] = (c, s, mn, mx)
            else:
                final[k] = v

    return final

def format_results(agg: dict) -> list[tuple[str, float, float, float]]:
    out = []
    for kb, (cnt, s, mn, mx) in agg.items():
        # 바이트 키는 필요 시 디코드(집계 시)
        name = kb.decode('utf-8', errors='ignore')
        # 정수(×10) → 소수
        avg = (s / cnt) / 10.0
        out.append((name, mn / 10.0, avg, mx / 10.0))
    # 정렬(알파벳 순)
    out.sort(key=lambda x: x[0])
    return out

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("파이썬 1brc 첼린지")
        sys.exit(1)

    path = sys.argv[1]
    n_chunks = int(sys.argv[2]) if len(sys.argv) > 2 else None
    n_procs = int(sys.argv[3]) if len(sys.argv) > 3 else None

    agg = aggregate_file(path, n_chunks, n_procs)
    rows = format_results(agg)
    
    for name, mn, avg, mx in rows[:20]:
        print(f"{name}: min={mn:.1f}, avg={avg:.1f}, max={mx:.1f}")
    print(f"total stations: {len(rows)}")
