# Trend Radar

Projeto de monitoramento de tendencias de tecnologias de IA usando Google Trends.

## Stack
- Python
- Polars
- DuckDB
- Plotly Dash

## Estrutura
- `src/`: pipeline, carga e analise de tendencias
- `dashboard/`: aplicacao interativa em Dash
- `data/`: base DuckDB local

## Como executar
1. Instale dependencias principais:
```bash
pip install -r requirements.txt
```
2. Instale dependencias do dashboard (caso nao estejam no seu ambiente):
```bash
pip install -r requirements-dashboard.txt
```
3. Rode o pipeline:
```bash
python -m src.pipeline
```
4. Rode o dashboard:
```bash
python dashboard/app.py
```
