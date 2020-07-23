import requests

api_base = "http://localhost:5000"

ep1 = "/api/v1/values/gene_info"
ep2 = "/api/v1/values/gpl_gse"
ep3 = "/api/v1/values/gse_values"

q1 = "%s%s?symbol=Bap18" % (api_base, ep1)

res = requests.get(q1)
data = res.json()
platforms_refs = data["platforms"]
gpls = list(platforms_refs.keys())
print(len(gpls))
for gpl in gpls:
    q2 = "%s%s?gpl=%s" % (api_base, ep2, gpl)

    res = requests.get(q2)
    gses = res.json()
    print(gses)
    for gse in gses:
        id_refs = platforms_refs[gpl]
        id_refs_str = ",".join(id_refs)
        q3 = "%s%s?gpl=%s&gse=%s&id_refs=%s" % (api_base, ep3, gpl, gse, id_refs_str)

        res = requests.get(q3)
        print(res.json())
        # break

    # break

