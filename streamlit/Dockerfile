FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances système si nécessaire
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copier et installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier l'application
COPY streamlit_app.py .

# Exposer le port Streamlit
EXPOSE 8501

# Healthcheck pour Cloud Run
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Commande pour lancer Streamlit
ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]

