# Dashboard Streamlit - Shopping Behavior Analytics

Dashboard de visualisation en temps réel des données depuis BigQuery pour le projet Spark Streaming.

## 🎯 Fonctionnalités

- 📊 **Métriques en temps réel** : Nombre de commandes, revenus, panier moyen, note moyenne
- 📈 **Visualisations interactives** :
  - Vue d'ensemble avec graphiques temporels
  - Analyse par tranche d'âge
  - Analyse par genre
  - Analyse par localisation
  - Analyses combinées (âge × genre × catégorie)
- 🔄 **Actualisation automatique** : Mise à jour périodique des données
- 🎨 **Interface moderne** : Utilise Plotly pour des graphiques interactifs

## 📋 Prérequis

- Python 3.8+
- Accès à BigQuery (project ID: `spark-streaming-483317`)
- Credentials GCP configurés

## 🚀 Installation

1. **Installer les dépendances** :

```bash
cd streamlit
pip install -r requirements.txt
```

2. **Configurer les variables d'environnement** :

```bash
# Copier le fichier d'exemple
cp .env.example .env

# Éditer .env avec vos valeurs
# Ou exporter les variables directement :
export GCP_PROJECT_ID=spark-streaming-483317
export BIGQUERY_DATASET=shopping
export BIGQUERY_TABLE=orders
```

3. **Configurer l'authentification GCP** :

**Option 1 : Application Default Credentials (recommandé pour Cloud Run/GCP)**

```bash
gcloud auth application-default login
```

**Option 2 : Service Account Key (pour développement local)**

```bash
# Télécharger le fichier JSON du Service Account depuis GCP Console
# Exporter le chemin :
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
```

## ▶️ Utilisation

### Lancement local

```bash
streamlit run streamlit_app.py
```

Le dashboard sera accessible à l'adresse : `http://localhost:8501`

### Déploiement sur Cloud Run (gratuit jusqu'à 2M requêtes/mois)

1. **Créer un Dockerfile** :

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streamlit_app.py .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

2. **Déployer sur Cloud Run** :

```bash
# Build et déployer
gcloud run deploy shopping-dashboard \
  --source . \
  --platform managed \
  --region europe-west1 \
  --allow-unauthenticated \
  --set-env-vars GCP_PROJECT_ID=spark-streaming-483317,BIGQUERY_DATASET=shopping
```

### Déploiement sur Streamlit Cloud (gratuit)

1. Pousser le code sur GitHub
2. Aller sur [streamlit.io/cloud](https://streamlit.io/cloud)
3. Connecter votre repository
4. Configurer les secrets dans Streamlit Cloud :
   - `GOOGLE_APPLICATION_CREDENTIALS` (contenu du fichier JSON du Service Account)

## 📊 Vues BigQuery utilisées

Le dashboard utilise les vues suivantes (créées via `bigquery_views.sql`) :

- `v_age_preferences` : Préférences par tranche d'âge
- `v_gender_preferences` : Préférences par genre
- `v_location_preferences` : Préférences par localisation
- `v_age_gender_category` : Analyse combinée âge × genre × catégorie

Si ces vues n'existent pas encore, créez-les en exécutant `bigquery_views.sql` dans BigQuery.

## ⚙️ Configuration

### Variables d'environnement

| Variable | Description | Défaut |
|----------|-------------|--------|
| `GCP_PROJECT_ID` | ID du projet GCP | `spark-streaming-483317` |
| `BIGQUERY_DATASET` | Nom du dataset BigQuery | `shopping` |
| `BIGQUERY_TABLE` | Nom de la table BigQuery | `orders` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Chemin vers le fichier de credentials (optionnel) | - |

### Paramètres dans l'interface

- **Actualisation automatique** : Active/désactive le rafraîchissement automatique
- **Intervalle** : Fréquence de mise à jour (5-60 secondes)
- **Filtres** : Filtrage par catégorie et localisation dans le tableau des commandes

## 🔍 Dépannage

### Erreur d'authentification

```
Error: Could not automatically determine credentials
```

**Solution** : Configurez les credentials GCP :
```bash
gcloud auth application-default login
```

### Aucune donnée affichée

- Vérifiez que le Consumer Spark a traité des fichiers
- Vérifiez que les données existent dans BigQuery :
  ```bash
  bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`spark-streaming-483317.shopping.orders\`"
  ```

### Vues BigQuery manquantes

Si vous voyez des avertissements sur les vues manquantes :
1. Exécutez `bigquery_views.sql` dans BigQuery Console
2. Ou modifiez le dataset dans `.env` si vous utilisez un dataset différent

## 📝 Notes

- Le cache des données est configuré avec un TTL (Time To Live) pour réduire les appels BigQuery
- Les requêtes utilisent les vues analytiques pour optimiser les performances
- Le dashboard affiche les dernières 10 000 commandes par défaut (configurable dans le code)

## 🆘 Support

Pour toute question ou problème, consultez :
- Documentation BigQuery : https://cloud.google.com/bigquery/docs
- Documentation Streamlit : https://docs.streamlit.io

