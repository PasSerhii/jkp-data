import csv, sys
csv.field_size_limit(10**9)
BASE = "/mnt/jkp-data/processed/production/monthly/"
for c in ["deu", "usa", "jpn"]:
    p = BASE + c + ".csv"
    with open(p, newline="") as f:
        r = csv.reader(f)
        h = next(r)
        iME, iMC = h.index("me"), h.index("me_company")
        iEOM, iRLL = h.index("eom"), h.index("ret_local_lead1m")
        iCONM, iCUSIP = h.index("conm"), h.index("cusip")
        n = may = jun = diff = rll = noname = 0
        for row in r:
            n += 1
            e = row[iEOM]
            tgt = e in ("20260531", "20260630")
            if e == "20260531":
                may += 1
                if not row[iCONM].strip():
                    noname += 1
            if e == "20260630":
                jun += 1
            if tgt:
                try:
                    if row[iMC] and row[iME] and abs(float(row[iMC]) - float(row[iME])) > 1e-4:
                        diff += 1
                except ValueError:
                    pass
                if row[iRLL].strip():
                    rll += 1
    print("%s: rows=%d may=%d jun=%d me_company_ne_me=%d ret_local_lead1m_nonnull=%d may_missing_conm=%d"
          % (c, n, may, jun, diff, rll, noname))
