# Guide de déploiement sur Streamlit Cloud

Ce guide vous explique comment déployer votre dashboard Streamlit sur [Streamlit Cloud](https://streamlit.io/cloud) (gratuit).

## 📋 Prérequis

1. Un compte GitHub
2. Un compte Streamlit Cloud (gratuit - connectez-vous avec GitHub)
3. Votre code poussé sur GitHub
4. Un Service Account GCP avec accès BigQuery

## 🚀 Étapes de déploiement

### 1. Préparer votre repository GitHub

Assurez-vous que votre code est sur GitHub :

```bash
# Si ce n'est pas déjà fait
git add .
git commit -m "Add Streamlit dashboard"
git push origin main
```

### 2. Créer un Service Account GCP pour Streamlit Cloud

Si vous n'avez pas encore de Service Account pour Streamlit :

```bash
# Créer un Service Account
gcloud iam service-accounts create streamlit-dashboard \
    --display-name="Streamlit Dashboard Service Account" \
    --project=spark-streaming-483317

# Donner les permissions BigQuery
gcloud projects add-iam-policy-binding spark-streaming-483317 \
    --member="serviceAccount:streamlit-dashboard@spark-streaming-483317.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding spark-streaming-483317 \
    --member="serviceAccount:streamlit-dashboard@spark-streaming-483317.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

# Créer et télécharger la clé JSON
gcloud iam service-accounts keys create streamlit-key.json \
    --iam-account=streamlit-dashboard@spark-streaming-483317.iam.gserviceaccount.com \
    --project=spark-streaming-483317
```

### 3. Se connecter à Streamlit Cloud

1. Allez sur https://streamlit.io/cloud
2. Cliquez sur "Sign in" et connectez-vous avec votre compte GitHub
3. Autorisez Streamlit Cloud à accéder à votre GitHub

### 4. Créer une nouvelle app

1. Cliquez sur "New app"
2. Remplissez le formulaire :
   - **Repository** : Sélectionnez votre repository GitHub
   - **Branch** : `main` (ou la branche où se trouve votre code)
   - **Main file path** : `streamlit/streamlit_app.py`
   - **App URL** : Choisissez un nom unique (ex: `shopping-behavior-dashboard`)

### 5. Configurer les secrets

**Important** : Les secrets permettent de stocker vos credentials GCP de manière sécurisée.

1. Dans la page de votre app, cliquez sur "⋮" (menu) → "Settings"
2. Allez dans la section "Secrets"
3. Ajoutez les secrets suivants :

#### Option A : Service Account JSON (Recommandé)

Copiez le contenu de votre fichier `streamlit-key.json` dans le secret :

```toml
[gcp_service_account]
type = "service_account"
project_id = "spark-streaming-483317"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "streamlit-dashboard@spark-streaming-483317.iam.gserviceaccount.com"
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "..."
```

**Ou** plus simplement, collez directement le JSON complet :

```toml
[gcp_service_account]
# Collez ici tout le contenu du fichier JSON, mais en format TOML
# Exemple :
type = "service_account"
project_id = "spark-streaming-483317"
private_key_id = "abc123..."
private_key = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC...\n-----END PRIVATE KEY-----\n"
client_email = "streamlit-dashboard@spark-streaming-483317.iam.gserviceaccount.com"
# ... etc (tout le contenu du JSON)
```

#### Option B : Variables d'environnement (Alternative)

Si vous préférez, vous pouvez aussi définir :

```toml
GCP_PROJECT_ID = "spark-streaming-483317"
BIGQUERY_DATASET = "shopping_dev"
BIGQUERY_TABLE = "orders"
```

4. Cliquez sur "Save"

### 6. Déployer

1. Streamlit Cloud déploiera automatiquement votre app
2. Vous verrez les logs de déploiement en temps réel
3. Une fois terminé, votre app sera disponible à l'URL : `https://<votre-nom-app>.streamlit.app`

### 7. Partager l'URL

Une fois déployé, vous pouvez partager l'URL avec n'importe qui. L'app est publique et accessible depuis n'importe où.

## 🔄 Mises à jour automatiques

Streamlit Cloud se met à jour automatiquement à chaque push sur la branche configurée. Vous n'avez rien à faire !

## 🐛 Dépannage

### Erreur : "Missing credentials"

- Vérifiez que les secrets sont correctement configurés
- Assurez-vous que le format TOML est correct
- Vérifiez que le Service Account a les bonnes permissions

### Erreur : "Permission denied"

Le Service Account n'a pas les permissions BigQuery. Vérifiez :

```bash
gcloud projects get-iam-policy spark-streaming-483317 \
    --flatten="bindings[].members" \
    --filter="bindings.members:streamlit-dashboard@spark-streaming-483317.iam.gserviceaccount.com"
```

### Erreur : "Module not found"

Vérifiez que `requirements.txt` contient toutes les dépendances nécessaires.

### Logs

Consultez les logs dans Streamlit Cloud pour voir les erreurs détaillées.

## 📝 Notes

- ✅ Streamlit Cloud est gratuit
- ✅ Déploiement automatique à chaque commit
- ✅ URL publique permanente
- ✅ Pas besoin de maintenir un serveur
- ✅ Scaling automatique

## 🔐 Sécurité

- ⚠️ Ne committez JAMAIS vos fichiers de credentials JSON dans Git
- ✅ Utilisez toujours les secrets Streamlit Cloud pour les credentials
- ✅ Limitez les permissions du Service Account au minimum nécessaire

## 📚 Ressources

- [Documentation Streamlit Cloud](https://docs.streamlit.io/streamlit-community-cloud)
- [Gestion des secrets](https://docs.streamlit.io/streamlit-community-cloud/get-started/deploy-an-app/connect-to-data-sources/secrets-management)

