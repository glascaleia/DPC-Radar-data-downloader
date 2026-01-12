
# RADAR-DPC – Client Python per download continuo dei prodotti radar (RAW)

Questo documento descrive **cosa fa**, **come installarlo** e **come usarlo** il client Python che
riceve notifiche in tempo reale via **WebSocket** e scarica i prodotti radar tramite **URL presigned**.
È pensato per **centri meteo** e **istituti di ricerca** che desiderano alimentare pipeline di
**nowcasting**, **archiviazione** o **post-processing** con i dati raw (GeoTIFF).

> Repository dei file forniti: `radar_downloader.py` e `requirements.txt` (link a fondo pagina).

---

## 1) Architettura in breve

```
(WebSocket)
   wss://radar-wss.protezionecivile.it
               ▼  messaggi: {"productType":"VMI","time":1758794400000,"period":"PT5M"}
        [Client Python]
               │  per i tipi desiderati invia:
               │  POST https://.../prod/downloadProduct
               │  body: {"productType":"VMI","productDate":1758794400000}
               ▼  risposta: { bucket, key, url, expiresSeconds }
     Presigned URL S3 (GET)
               │
               ▼
       Salvataggio file locale rispettando la `key`
       es: ./downloads/VMI/22-09-2025-11-40.tif
```

Caratteristiche principali:
- **Filtro prodotti** configurabile (es. `VMI,SRI,SRT1,TEMP,...`).
- **Download in parallelo** (worker pool) e ripresa idempotente (saltiamo file già presenti).
- **Percorsi sicuri**: la `key` S3 viene sanificata e le directory create automaticamente.

---

## 2) Requisiti

- Python 3.9+ (consigliato 3.10/3.11)
- Accesso in uscita verso:
  - `wss://radar-wss.protezionecivile.it`
  - `https://radar-api.protezionecivile.it/downloadProduct`
- Spazio su disco adeguato (GeoTIFF ~0.5 MB ciascuno in media)
- Facoltativo: systemd (Linux) o Docker per esecuzione come servizio

---

## 3) Installazione rapida

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

I file necessari sono:
- `radar_downloader.py`
- `requirements.txt`

> Vedi sezione **Download file** per i link diretti.

---

## 4) Utilizzo base

Scarica VMI, SRI e TEMP in `./downloads`:

```bash
python radar_downloader.py --products VMI,SRI,TEMP --output ./downloads
```

Log più dettagliati:
```bash
python radar_downloader.py --products VMI,SRI,TEMP --output ./downloads --log-level DEBUG
```

### Parametri principali (CLI)

- `--products` lista separata da virgola, es: `VMI,SRI,SRT1,TEMP`
- `--output` cartella di destinazione (default: `./downloads`)
- `--workers` numero download paralleli (default: `3`)
- `--log-level` livelli `DEBUG, INFO, WARNING, ERROR` (default: `INFO`)

### Variabili d’ambiente equivalenti (opzionali)

- `RADAR_WS_URL` (default `wss://radar-wss.protezionecivile.it`)
- `RADAR_API_ENDPOINT` (default `https://radar-api.protezionecivile.it/downloadProduct`)
- `RADAR_PRODUCTS` (default `VMI,SRI,TEMP`)
- `RADAR_OUTPUT_DIR` (default `./downloads`)

Esempio:
```bash
export RADAR_PRODUCTS="VMI,SRI,SRT1"
export RADAR_OUTPUT_DIR="/data/radar"
python radar_downloader.py
```

---

## 5) Flusso operativo

1. **Connessione WebSocket**
2. **Ricezione evento** (JSON): `{"productType":"VMI","time":<epoch_ms>,"period":"PT5M"}`.
3. Se `productType` è nella lista ammessa, il client invia `POST` al servizio `downloadProduct` con:
   ```json
   {"productType":"VMI","productDate":1758794400000}
   ```
4. Il servizio risponde con:
   ```json
   {"bucket":"dpc-radar","key":"VMI/22-09-2025-11-40.tif","url":"<presigned-url>","expiresSeconds":300}
   ```
5. Il client **scarica** dal `url` e **salva** il file in `OUTPUT_DIR/<key>`. Se il file esiste già con size>0, lo **salta**.

Note:
- La **deduplicazione** evita doppio download dello stesso (productType, timestamp).
- I **worker paralleli** accelerano il throughput senza sovraccaricare la rete.

---

## 6) Mappature prodotto & frequenze (indicative)

- Radar riflettività: `VMI` (5 min), `SRI` (5 min), `SRT1` (5 min)
- Altri prodotti radar: a frequenza 30 min
- Temperatura (ad es. satellitare): `TEMP` (60 min)

> La frequenza non è vincolante per il client: segue ciò che viene pubblicato sul topic.

---

## 7) Esecuzione come servizio (systemd)

**Unit file** di esempio (`/etc/systemd/system/radar-downloader.service`):

```ini
[Unit]
Description=RADAR-DPC continuous downloader
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=radar
Group=radar
WorkingDirectory=/opt/radar-downloader
Environment=RADAR_PRODUCTS=VMI,SRI,SRT1,TEMP
Environment=RADAR_OUTPUT_DIR=/data/radar
ExecStart=/opt/radar-downloader/.venv/bin/python /opt/radar-downloader/radar_downloader.py --log-level INFO
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Comandi utili:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now radar-downloader
sudo journalctl -u radar-downloader -f
```

---

## 8) Esecuzione in Docker (opzionale)

**Dockerfile** minimale:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY radar_downloader.py .
ENV RADAR_PRODUCTS=VMI,SRI,TEMP
ENV RADAR_OUTPUT_DIR=/data
VOLUME ["/data"]
CMD ["python", "radar_downloader.py", "--log-level", "INFO"]
```

Build & run:
```bash
docker build -t radar-downloader:latest .
docker run --name radar -e RADAR_PRODUCTS="VMI,SRI,SRT1" -v /data/radar:/data --restart=always radar-downloader:latest
```

---

## 9) Sicurezza e stabilità

- **Presigned URL**: hanno scadenza breve (`expiresSeconds`, tipicamente 300s). Il client scarica subito e non conserva le URL.
- **Path traversal**: la `key` viene sanificata, impedendo scritture fuori da `OUTPUT_DIR`.
- **Reconnect/backoff**: al drop della connessione, il client attende con backoff esponenziale (fino a 30s) prima di riconnettersi.
- **Idempotenza**: se un file esiste con dimensione > 0 non viene riscaricato.
- **Timeout**: POST e GET hanno timeout ragionevoli per non rimanere bloccati.

---

## 10) Performance tuning

- Aumenta `--workers` se la banda lo consente.
- Usa filesystem locali veloci per la directory `OUTPUT_DIR`.
- Esegui su host vicino alla regione `eu-south-1` per ridurre la latenza delle presigned URL.

---

## 11) Licenza & contatti

Questo client è fornito con licenza CC-BY-SA. Adattalo al tuo ambiente operativo e alle policy del tuo ente.
Per supporto o feature aggiuntive, contatta i maintainer del progetto.
Lab GEOSDI CNR IMAA

---

## Download file

- **Client**: [radar_downloader.py](sandbox:/mnt/data/radar_downloader.py)  
- **Dipendenze**: [requirements.txt](sandbox:/mnt/data/requirements.txt)

