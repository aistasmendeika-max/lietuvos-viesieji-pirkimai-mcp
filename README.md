# Lietuvos viešųjų pirkimų MCP

MCP serveris darbui su Lietuvos viešųjų pirkimų duomenimis.

Pagrindinis šaltinis – naujosios CVP IS viešas API:
`https://viesiejipirkimai.lt/epps-integration/api/cft-details-export`

Serveris skirtas ChatGPT / Claude / kitam MCP klientui:
- gauti naujos CVP IS pirkimų puslapius per oficialų VPT API;
- ieškoti gautuose pirkimuose pagal tekstą;
- filtruoti pagal pirkimo numerį, organizaciją, tiekėją ar kitą tekstą;
- grąžinti pirminius JSON duomenis tolesnei analizei;
- pateikti nuorodas į senąją ir naująją CVP IS paiešką.

## Reikalavimai

- Python 3.11+
- VPT CVP IS API raktas

2026-05-20 VPT paskelbė API naudojimo instrukciją ir nurodė, kad užklausa siunčiama POST metodu į `cft-details-export`, perduodant `apiKey` antraštėje.

## Diegimas

```bash
git clone https://github.com/aistasmendeika-max/lietuvos-viesieji-pirkimai-mcp.git
cd lietuvos-viesieji-pirkimai-mcp

python -m venv .venv
```

Windows:

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Į `.env` įrašykite VPT API raktą:

```env
VPT_API_KEY=JUSU_VPT_API_RAKTAS
```

Paleidimas:

```powershell
python server.py
```

## MCP įrankiai

### `cvpis_page`
Paima vieną oficialaus naujos CVP IS API rezultatų puslapį.

Parametrai:
- `page_num` – puslapio numeris;
- `page_size` – įrašų skaičius puslapyje.

### `search_cvpis`
Peržiūri kelis CVP IS API puslapius ir atrenka įrašus, kurių JSON tekste yra paieškos frazė.

Pavyzdinės frazės:
- `Kretingos rajono savivaldybė`
- `Rudkasa`
- `sniego`
- konkretus pirkimo numeris

### `procurement_sources`
Grąžina pagrindinius oficialius viešųjų pirkimų šaltinius.

## Pastaba

CVP IS API struktūrą valdo Viešųjų pirkimų tarnyba. Jei VPT pakeis laukų pavadinimus ar API parametrus, serverio adapterį gali reikėti atnaujinti.

Projektas nepakeičia oficialių CVP IS duomenų ir teisinių dokumentų. Tyrimuose visada tikrinkite pirminį šaltinį.
