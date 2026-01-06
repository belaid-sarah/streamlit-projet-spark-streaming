"""
Dashboard Streamlit pour visualisation en streaming des données BigQuery
Affiche les données depuis la table orders et les vues analytiques BigQuery
"""

import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

# Charger les variables d'environnement (pour développement local)
load_dotenv()

# Configuration - Supporte à la fois les secrets Streamlit Cloud et les variables d'environnement locales
# Streamlit Cloud utilise st.secrets, développement local utilise os.getenv()
def get_config(key: str, default: str = "") -> str:
    """Récupère une configuration depuis Streamlit secrets (Cloud) ou variables d'environnement (local)"""
    try:
        # Essayer d'abord les secrets Streamlit Cloud
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    
    # Sinon, utiliser les variables d'environnement (développement local)
    return os.getenv(key, default)

PROJECT_ID = get_config("GCP_PROJECT_ID", "spark-streaming-483317")
DATASET = get_config("BIGQUERY_DATASET", "shopping_dev")  # Par défaut: shopping_dev (environnement dev)
TABLE = get_config("BIGQUERY_TABLE", "orders")

# Authentification BigQuery
@st.cache_resource
def init_bigquery_client():
    """Initialise le client BigQuery en utilisant les credentials GCP"""
    try:
        # Option 1: Credentials depuis Streamlit Secrets (Streamlit Cloud)
        try:
            if hasattr(st, 'secrets'):
                # Essayer d'accéder aux secrets (peut échouer si le fichier TOML est mal formaté)
                try:
                    if 'gcp_service_account' in st.secrets:
                        credentials = service_account.Credentials.from_service_account_info(
                            st.secrets['gcp_service_account'],
                            scopes=["https://www.googleapis.com/auth/bigquery"]
                        )
                        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
                        return client
                except Exception as secrets_error:
                    # Erreur de parsing TOML ou autre erreur liée aux secrets
                    error_msg = str(secrets_error)
                    if "secrets.toml" in error_msg or "parsing" in error_msg.lower() or "TOML" in error_msg:
                        st.error("❌ **Erreur dans le fichier secrets.toml**")
                        st.error(f"**Détails:** {error_msg}")
                        st.markdown("""
                        **Le fichier secrets.toml a une erreur de syntaxe TOML.**
                        
                        **Pour corriger dans Streamlit Cloud :**
                        1. Allez dans **Settings** → **Secrets**
                        2. Vérifiez le format TOML. Voici un exemple correct :
                        
                        ```toml
                        [gcp_service_account]
                        type = "service_account"
                        project_id = "spark-streaming-483317"
                        private_key_id = "VOTRE_PRIVATE_KEY_ID"
                        private_key = "-----BEGIN PRIVATE KEY-----\\nVOTRE_KEY\\n-----END PRIVATE KEY-----\\n"
                        client_email = "votre-email@spark-streaming-483317.iam.gserviceaccount.com"
                        client_id = "VOTRE_CLIENT_ID"
                        auth_uri = "https://accounts.google.com/o/oauth2/auth"
                        token_uri = "https://oauth2.googleapis.com/token"
                        auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
                        client_x509_cert_url = "VOTRE_CERT_URL"
                        ```
                        
                        **Points importants :**
                        - Utilisez `\\n` (double backslash) pour les sauts de ligne dans `private_key`
                        - Pas d'espaces avant ou après les `=`
                        - Les valeurs de chaîne doivent être entre guillemets
                        - Chaque ligne doit avoir une valeur après le `=`
                        """)
                        raise  # Re-lancer pour arrêter l'exécution
                    # Autre erreur, continuer vers les autres options
                    pass
        except (AttributeError, KeyError):
            pass  # Continuer vers les autres options
        
        # Option 2: Fichier de service account (développement local)
        service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", None)
        if service_account_path and os.path.exists(service_account_path):
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
            return client
        
        # Option 3: Application Default Credentials (recommandé sur GCP/Cloud Run)
        # Forcer explicitement le project ID pour éviter les conflits avec d'anciens projets
        import google.auth
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        # S'assurer que les credentials utilisent le bon projet
        if hasattr(credentials, 'project_id') and credentials.project_id != PROJECT_ID:
            credentials.project_id = PROJECT_ID
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
        # Tester la connexion (optionnel - peut échouer si le dataset n'existe pas encore)
        try:
            client.get_dataset(f"{PROJECT_ID}.{DATASET}")
        except Exception:
            pass  # Ignorer l'erreur si le dataset n'existe pas encore
        
        return client
    except Exception as e:
        error_msg = str(e)
        st.error(f"❌ Erreur d'authentification BigQuery: {error_msg}")
        
        if "secrets.toml" in error_msg or "parsing" in error_msg.lower() or "TOML" in error_msg:
            # Déjà géré plus haut, ne rien faire
            pass
        elif "403" in error_msg or "401" in error_msg or "CREDENTIALS" in error_msg:
            st.error("**Problème d'authentification détecté**")
            st.markdown("""
            **Solutions possibles :**
            
            1. **Pour Streamlit Cloud - Configurer les secrets :**
               - Allez dans **Settings** → **Secrets**
               - Ajoutez la configuration `[gcp_service_account]` (voir `secrets.example.toml`)
            
            2. **Pour développement local - Configurer Application Default Credentials :**
               ```powershell
               gcloud auth application-default login
               gcloud config set project spark-streaming-483317
               ```
            
            3. **OU utiliser un fichier Service Account :**
               - Téléchargez un fichier JSON de Service Account depuis GCP Console
               - Définissez la variable d'environnement :
                 ```powershell
                 $env:GOOGLE_APPLICATION_CREDENTIALS="C:\\chemin\\vers\\service-account-key.json"
                 ```
            
            4. **Vérifier que le projet est actif :**
               ```powershell
               gcloud config get-value project
               gcloud projects describe spark-streaming-483317
               ```
            """)
        raise

@st.cache_data(ttl=10)  # Cache pendant 10 secondes pour réduire les appels BigQuery
def fetch_latest_orders(limit=1000):
    """Récupère les dernières commandes depuis BigQuery avec les colonnes enrichies"""
    client = init_bigquery_client()
    
    query = f"""
    SELECT 
        customer_id,
        age,
        gender,
        category,
        item_purchased,
        purchase_amount_usd,
        location,
        review_rating,
        subscription_status,
        payment_method,
        processed_time,
        final_amount_usd,
        amount_category,
        customer_segment,
        satisfaction_level,
        is_anomaly,
        estimated_clv,
        frequency_category,
        estimated_profit_usd,
        season_type,
        loyalty_score
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    ORDER BY processed_time DESC
    LIMIT {limit}
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        error_msg = str(e)
        if "db-dtypes" in error_msg.lower():
            st.error("❌ **Package manquant : db-dtypes**")
            st.markdown("""
            **Pour résoudre ce problème :**
            
            **Si vous êtes sur Streamlit Cloud :**
            1. Vérifiez que vous avez poussé les modifications de `requirements.txt` sur GitHub
            2. Attendez que Streamlit Cloud redéploie automatiquement (quelques minutes)
            3. Vérifiez les logs pour confirmer l'installation du package
            
            **Si vous testez en local :**
            ```bash
            pip install db-dtypes
            # Ou réinstaller toutes les dépendances :
            pip install -r requirements.txt
            ```
            """)
        else:
            st.error(f"Erreur lors de la récupération des commandes: {error_msg}")
        return pd.DataFrame()

@st.cache_data(ttl=30)  # Cache pendant 30 secondes
def fetch_age_preferences():
    """Récupère les données depuis la vue v_age_preferences"""
    client = init_bigquery_client()
    
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{DATASET}.v_age_preferences`
    ORDER BY age_bucket
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        # Si la vue n'existe pas, on retourne un DataFrame vide
        st.warning(f"Vue v_age_preferences non disponible: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=30)
def fetch_gender_preferences():
    """Récupère les données depuis la vue v_gender_preferences"""
    client = init_bigquery_client()
    
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{DATASET}.v_gender_preferences`
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.warning(f"Vue v_gender_preferences non disponible: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=30)
def fetch_location_preferences():
    """Récupère les données depuis la vue v_location_preferences"""
    client = init_bigquery_client()
    
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{DATASET}.v_location_preferences`
    ORDER BY orders DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.warning(f"Vue v_location_preferences non disponible: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=30)
def fetch_age_gender_category():
    """Récupère les données depuis la vue v_age_gender_category"""
    client = init_bigquery_client()
    
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{DATASET}.v_age_gender_category`
    ORDER BY orders DESC
    LIMIT 50
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.warning(f"Vue v_age_gender_category non disponible: {str(e)}")
        return pd.DataFrame()

# Configuration de la page
st.set_page_config(
    page_title="Shopping Behavior Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Titre principal
st.title("🛒 Shopping Behavior Analytics - Temps Réel")
st.markdown("Visualisation en streaming des données de commandes depuis BigQuery")
st.markdown("---")

# Sidebar pour les contrôles
with st.sidebar:
    st.header("⚙️ Configuration")
    
    st.info(f"**Project:** {PROJECT_ID}\n\n**Dataset:** {DATASET}\n\n**Table:** {TABLE}")
    
    auto_refresh = st.checkbox("🔄 Actualisation automatique", value=True)
    refresh_interval = st.slider("Intervalle (secondes)", 5, 60, 15)
    
    if auto_refresh:
        st.info(f"Actualisation toutes les {refresh_interval} secondes")
    
    if st.button("🔄 Actualiser maintenant"):
        st.cache_data.clear()
        st.success("Données actualisées!")
    
    st.markdown("---")
    st.markdown("### 📊 Informations")
    st.markdown("Ce dashboard affiche les données en temps réel depuis BigQuery.")
    st.markdown("Les données sont mises à jour automatiquement par le Consumer Spark.")

# Métriques principales
st.subheader("📊 Métriques en temps réel")

try:
    orders_df = fetch_latest_orders(limit=10000)
    
    if orders_df.empty:
        st.warning("⚠️ Aucune donnée trouvée dans BigQuery. Vérifiez que le Consumer a traité des fichiers.")
    else:
        # Première ligne de métriques
        col1, col2, col3, col4 = st.columns(4)
        
        total_orders = len(orders_df)
        total_revenue = orders_df['purchase_amount_usd'].sum() if 'purchase_amount_usd' in orders_df.columns else 0
        avg_order_value = orders_df['purchase_amount_usd'].mean() if 'purchase_amount_usd' in orders_df.columns else 0
        avg_rating = orders_df['review_rating'].mean() if 'review_rating' in orders_df.columns else 0
        
        col1.metric("Total Commandes", f"{total_orders:,}")
        col2.metric("Revenus Total", f"${total_revenue:,.2f}")
        col3.metric("Panier Moyen", f"${avg_order_value:.2f}")
        col4.metric("Note Moyenne", f"{avg_rating:.2f}")
        
        # Deuxième ligne de métriques (nouvelles colonnes enrichies)
        col5, col6, col7, col8 = st.columns(4)
        
        # Métriques basées sur les nouvelles colonnes
        anomalies_count = orders_df['is_anomaly'].sum() if 'is_anomaly' in orders_df.columns else 0
        total_profit = orders_df['estimated_profit_usd'].sum() if 'estimated_profit_usd' in orders_df.columns else 0
        vip_customers = len(orders_df[orders_df['customer_segment'] == 'VIP']) if 'customer_segment' in orders_df.columns else 0
        final_revenue = orders_df['final_amount_usd'].sum() if 'final_amount_usd' in orders_df.columns else total_revenue
        
        col5.metric("🚨 Anomalies", f"{anomalies_count:,}", delta=f"{(anomalies_count/total_orders*100):.1f}%" if total_orders > 0 else "0%")
        col6.metric("💰 Profit Estimé", f"${total_profit:,.2f}")
        col7.metric("👑 Clients VIP", f"{vip_customers:,}")
        col8.metric("💵 Revenus Finaux", f"${final_revenue:,.2f}")
        
        # Graphiques dans des onglets
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
            "📈 Vue d'ensemble", 
            "👥 Par Âge", 
            "⚧️ Par Genre", 
            "🌍 Par Localisation",
            "🔀 Combinaisons",
            "👑 Clients VIP & Premium",
            "🚨 Détection d'Anomalies",
            "📊 Analyse Avancée"
        ])
        
        with tab1:
            st.subheader("Dernières commandes - Vue d'ensemble")
            
            # Graphique temporal des commandes
            if 'processed_time' in orders_df.columns:
                orders_df['processed_time'] = pd.to_datetime(orders_df['processed_time'])
                orders_df['hour'] = orders_df['processed_time'].dt.floor('H')
                hourly_orders = orders_df.groupby('hour').agg({
                    'purchase_amount_usd': ['count', 'sum']
                }).reset_index()
                hourly_orders.columns = ['hour', 'count', 'revenue']
                
                col_left, col_right = st.columns(2)
                
                with col_left:
                    fig1 = go.Figure()
                    fig1.add_trace(go.Scatter(
                        x=hourly_orders['hour'],
                        y=hourly_orders['count'],
                        mode='lines+markers',
                        name='Nombre de commandes',
                        line=dict(color='#1f77b4', width=2),
                        fill='tonexty'
                    ))
                    fig1.update_layout(
                        title="Évolution du nombre de commandes (par heure)",
                        xaxis_title="Heure",
                        yaxis_title="Nombre de commandes",
                        hovermode='x unified',
                        height=400
                    )
                    st.plotly_chart(fig1, use_container_width=True)
                
                with col_right:
                    fig2 = go.Figure()
                    fig2.add_trace(go.Scatter(
                        x=hourly_orders['hour'],
                        y=hourly_orders['revenue'],
                        mode='lines+markers',
                        name='Revenus (USD)',
                        line=dict(color='#2ca02c', width=2),
                        fill='tonexty'
                    ))
                    fig2.update_layout(
                        title="Évolution des revenus (par heure)",
                        xaxis_title="Heure",
                        yaxis_title="Revenus (USD)",
                        hovermode='x unified',
                        height=400
                    )
                    st.plotly_chart(fig2, use_container_width=True)
            
            # Top catégories
            st.markdown("### Top Catégories")
            col_cat1, col_cat2 = st.columns(2)
            
            with col_cat1:
                category_counts = orders_df['category'].value_counts().head(10)
                fig3 = px.bar(
                    x=category_counts.values,
                    y=category_counts.index,
                    orientation='h',
                    title="Top 10 Catégories (Volume)",
                    labels={'x': 'Nombre de commandes', 'y': 'Catégorie'},
                    color=category_counts.values,
                    color_continuous_scale='Blues'
                )
                fig3.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
                st.plotly_chart(fig3, use_container_width=True)
            
            with col_cat2:
                category_revenue = orders_df.groupby('category')['purchase_amount_usd'].sum().sort_values(ascending=False).head(10)
                fig4 = px.bar(
                    x=category_revenue.values,
                    y=category_revenue.index,
                    orientation='h',
                    title="Top 10 Catégories (Revenus)",
                    labels={'x': 'Revenus (USD)', 'y': 'Catégorie'},
                    color=category_revenue.values,
                    color_continuous_scale='Greens'
                )
                fig4.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
                st.plotly_chart(fig4, use_container_width=True)
            
            # Distribution des montants
            st.markdown("### Distribution des Montants")
            fig5 = px.histogram(
                orders_df,
                x='purchase_amount_usd',
                nbins=50,
                title="Distribution des montants de commandes",
                labels={'purchase_amount_usd': 'Montant (USD)', 'count': 'Nombre de commandes'}
            )
            st.plotly_chart(fig5, use_container_width=True)
        
        with tab2:
            st.subheader("Préférences par tranche d'âge")
            age_df = fetch_age_preferences()
            
            if not age_df.empty:
                col_a1, col_a2 = st.columns(2)
                
                with col_a1:
                    fig_age1 = px.bar(
                        age_df,
                        x='age_bucket',
                        y='orders',
                        title="Nombre de commandes par tranche d'âge",
                        labels={'orders': 'Nombre de commandes', 'age_bucket': 'Tranche d\'âge'},
                        color='orders',
                        color_continuous_scale='Viridis'
                    )
                    st.plotly_chart(fig_age1, use_container_width=True)
                
                with col_a2:
                    fig_age2 = px.bar(
                        age_df,
                        x='age_bucket',
                        y='avg_spend',
                        title="Dépense moyenne par tranche d'âge",
                        labels={'avg_spend': 'Dépense moyenne (USD)', 'age_bucket': 'Tranche d\'âge'},
                        color='avg_spend',
                        color_continuous_scale='Plasma'
                    )
                    st.plotly_chart(fig_age2, use_container_width=True)
                
                col_a3, col_a4 = st.columns(2)
                
                with col_a3:
                    fig_age3 = px.bar(
                        age_df,
                        x='age_bucket',
                        y='avg_rating',
                        title="Note moyenne par tranche d'âge",
                        labels={'avg_rating': 'Note moyenne', 'age_bucket': 'Tranche d\'âge'},
                        color='avg_rating',
                        color_continuous_scale='RdYlGn'
                    )
                    st.plotly_chart(fig_age3, use_container_width=True)
                
                with col_a4:
                    if 'top_category' in age_df.columns:
                        fig_age4 = px.bar(
                            age_df,
                            x='age_bucket',
                            y='top_category',
                            orientation='h',
                            title="Catégorie préférée par tranche d'âge",
                            labels={'top_category': 'Catégorie', 'age_bucket': 'Tranche d\'âge'},
                            color='age_bucket'
                        )
                        st.plotly_chart(fig_age4, use_container_width=True)
                
                st.markdown("### Tableau détaillé")
                st.dataframe(age_df, use_container_width=True, hide_index=True)
            else:
                st.info("Les vues analytiques ne sont pas encore disponibles. Assurez-vous que les vues BigQuery sont créées.")
        
        with tab3:
            st.subheader("Préférences par genre")
            gender_df = fetch_gender_preferences()
            
            if not gender_df.empty:
                col_g1, col_g2 = st.columns(2)
                
                with col_g1:
                    fig_gen1 = px.pie(
                        gender_df,
                        values='orders',
                        names='gender',
                        title="Répartition des commandes par genre",
                        hole=0.4
                    )
                    st.plotly_chart(fig_gen1, use_container_width=True)
                
                with col_g2:
                    fig_gen2 = px.bar(
                        gender_df,
                        x='gender',
                        y='avg_spend',
                        title="Dépense moyenne par genre",
                        labels={'avg_spend': 'Dépense moyenne (USD)', 'gender': 'Genre'},
                        color='gender',
                        color_discrete_sequence=px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig_gen2, use_container_width=True)
                
                col_g3, col_g4 = st.columns(2)
                
                with col_g3:
                    fig_gen3 = px.bar(
                        gender_df,
                        x='gender',
                        y='avg_rating',
                        title="Note moyenne par genre",
                        labels={'avg_rating': 'Note moyenne', 'gender': 'Genre'},
                        color='gender',
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    st.plotly_chart(fig_gen3, use_container_width=True)
                
                with col_g4:
                    if 'top_category' in gender_df.columns:
                        fig_gen4 = px.bar(
                            gender_df,
                            x='gender',
                            y='top_category',
                            orientation='h',
                            title="Catégorie préférée par genre",
                            labels={'top_category': 'Catégorie', 'gender': 'Genre'},
                            color='gender',
                            color_discrete_sequence=px.colors.qualitative.Pastel
                        )
                        st.plotly_chart(fig_gen4, use_container_width=True)
                
                st.markdown("### Tableau détaillé")
                st.dataframe(gender_df, use_container_width=True, hide_index=True)
            else:
                st.info("Les vues analytiques ne sont pas encore disponibles.")
        
        with tab4:
            st.subheader("Préférences par localisation")
            location_df = fetch_location_preferences()
            
            if not location_df.empty:
                fig_loc1 = px.bar(
                    location_df.head(20),
                    x='location',
                    y='orders',
                    title="Top 20 Localisations (Nombre de commandes)",
                    labels={'orders': 'Nombre de commandes', 'location': 'Localisation'},
                    color='orders',
                    color_continuous_scale='Blues'
                )
                fig_loc1.update_xaxes(tickangle=45)
                st.plotly_chart(fig_loc1, use_container_width=True)
                
                col_loc1, col_loc2 = st.columns(2)
                
                with col_loc1:
                    fig_loc2 = px.bar(
                        location_df.head(15),
                        x='location',
                        y='avg_spend',
                        title="Dépense moyenne par localisation (Top 15)",
                        labels={'avg_spend': 'Dépense moyenne (USD)', 'location': 'Localisation'},
                        color='avg_spend',
                        color_continuous_scale='Greens'
                    )
                    fig_loc2.update_xaxes(tickangle=45)
                    st.plotly_chart(fig_loc2, use_container_width=True)
                
                with col_loc2:
                    # Graphique en treemap si disponible
                    if len(location_df) > 0:
                        fig_loc3 = px.treemap(
                            location_df.head(20),
                            path=['location'],
                            values='orders',
                            title="Répartition des commandes par localisation (Treemap)",
                            color='avg_spend',
                            color_continuous_scale='Viridis'
                        )
                        st.plotly_chart(fig_loc3, use_container_width=True)
                
                st.markdown("### Tableau détaillé")
                st.dataframe(location_df, use_container_width=True, hide_index=True)
            else:
                st.info("Les vues analytiques ne sont pas encore disponibles.")
        
        with tab5:
            st.subheader("Analyse combinée Âge × Genre × Catégorie")
            age_gender_df = fetch_age_gender_category()
            
            if not age_gender_df.empty:
                # Heatmap
                pivot_df = age_gender_df.pivot_table(
                    index='age_bucket',
                    columns='gender',
                    values='orders',
                    aggfunc='sum',
                    fill_value=0
                )
                
                fig_heat = px.imshow(
                    pivot_df,
                    labels=dict(x="Genre", y="Tranche d'âge", color="Nombre de commandes"),
                    title="Heatmap: Commandes par Âge et Genre",
                    color_continuous_scale='YlOrRd',
                    aspect="auto"
                )
                st.plotly_chart(fig_heat, use_container_width=True)
                
                # Graphique 3D ou barres groupées
                fig_comb = px.bar(
                    age_gender_df.head(30),
                    x='category',
                    y='orders',
                    color='gender',
                    facet_row='age_bucket',
                    title="Commandes par Catégorie, Genre et Âge (Top 30)",
                    labels={'orders': 'Nombre de commandes', 'category': 'Catégorie'}
                )
                fig_comb.update_xaxes(tickangle=45)
                st.plotly_chart(fig_comb, use_container_width=True)
                
                st.markdown("### Tableau détaillé")
                st.dataframe(age_gender_df, use_container_width=True, hide_index=True)
            else:
                st.info("Les vues analytiques ne sont pas encore disponibles.")
        
        with tab6:
            st.subheader("👑 Analyse des Clients VIP avec Achats Premium")
            
            # Filtrer les clients VIP avec achats Premium
            if 'customer_segment' in orders_df.columns and 'amount_category' in orders_df.columns:
                vip_premium_df = orders_df[(orders_df['customer_segment'] == 'VIP') & 
                                          (orders_df['amount_category'] == 'Premium')].copy()
                
                if not vip_premium_df.empty:
                    col_vip1, col_vip2 = st.columns(2)
                    
                    with col_vip1:
                        st.metric("Nombre de transactions VIP Premium", len(vip_premium_df))
                        st.metric("Revenus VIP Premium", f"${vip_premium_df['purchase_amount_usd'].sum():,.2f}")
                    
                    with col_vip2:
                        avg_vip_premium = vip_premium_df['purchase_amount_usd'].mean()
                        avg_clv_vip = vip_premium_df['estimated_clv'].mean() if 'estimated_clv' in vip_premium_df.columns else 0
                        st.metric("Panier Moyen VIP Premium", f"${avg_vip_premium:,.2f}")
                        st.metric("CLV Moyen", f"${avg_clv_vip:,.2f}")
                    
                    # Répartition par catégorie pour VIP Premium
                    st.markdown("### Répartition par Catégorie")
                    vip_category = vip_premium_df.groupby('category').agg({
                        'purchase_amount_usd': ['count', 'sum', 'mean'],
                        'estimated_clv': 'mean' if 'estimated_clv' in vip_premium_df.columns else 'count'
                    }).reset_index()
                    vip_category.columns = ['category', 'count', 'total_revenue', 'avg_amount', 'avg_clv']
                    vip_category = vip_category.sort_values('total_revenue', ascending=False)
                    
                    col_vip3, col_vip4 = st.columns(2)
                    
                    with col_vip3:
                        fig_vip1 = px.bar(
                            vip_category,
                            x='category',
                            y='total_revenue',
                            title="Revenus par Catégorie (VIP Premium)",
                            labels={'total_revenue': 'Revenus (USD)', 'category': 'Catégorie'},
                            color='total_revenue',
                            color_continuous_scale='Gold'
                        )
                        fig_vip1.update_xaxes(tickangle=45)
                        st.plotly_chart(fig_vip1, use_container_width=True)
                    
                    with col_vip4:
                        if 'loyalty_score' in vip_premium_df.columns:
                            loyalty_dist = vip_premium_df['loyalty_score'].value_counts()
                            fig_vip2 = px.pie(
                                values=loyalty_dist.values,
                                names=loyalty_dist.index,
                                title="Répartition par Score de Fidélité (VIP Premium)",
                                hole=0.4
                            )
                            st.plotly_chart(fig_vip2, use_container_width=True)
                    
                    st.markdown("### Tableau détaillé VIP Premium")
                    display_cols_vip = ['processed_time', 'customer_id', 'category', 'item_purchased',
                                       'purchase_amount_usd', 'estimated_clv', 'loyalty_score', 
                                       'frequency_category', 'location']
                    available_cols_vip = [col for col in display_cols_vip if col in vip_premium_df.columns]
                    st.dataframe(vip_premium_df[available_cols_vip].head(100), use_container_width=True, hide_index=True)
                else:
                    st.info("Aucune transaction VIP Premium trouvée.")
            else:
                st.warning("Les colonnes customer_segment et amount_category ne sont pas disponibles.")
        
        with tab7:
            st.subheader("🚨 Détection des Transactions Suspectes (Anomalies)")
            
            if 'is_anomaly' in orders_df.columns:
                anomalies_df = orders_df[orders_df['is_anomaly'] == True].copy()
                
                if not anomalies_df.empty:
                    col_anom1, col_anom2, col_anom3 = st.columns(3)
                    
                    with col_anom1:
                        st.metric("🚨 Transactions Anormales", len(anomalies_df))
                    with col_anom2:
                        anomaly_rate = (len(anomalies_df) / len(orders_df)) * 100 if len(orders_df) > 0 else 0
                        st.metric("Taux d'Anomalies", f"{anomaly_rate:.2f}%")
                    with col_anom3:
                        st.metric("Montant Total Anormal", f"${anomalies_df['purchase_amount_usd'].sum():,.2f}")
                    
                    st.markdown("### Distribution des Anomalies")
                    col_anom4, col_anom5 = st.columns(2)
                    
                    with col_anom4:
                        # Anomalies par catégorie
                        anom_by_category = anomalies_df.groupby('category')['purchase_amount_usd'].agg(['count', 'sum']).reset_index()
                        anom_by_category.columns = ['category', 'count', 'total']
                        anom_by_category = anom_by_category.sort_values('total', ascending=False)
                        
                        fig_anom1 = px.bar(
                            anom_by_category,
                            x='category',
                            y='total',
                            title="Montant des Anomalies par Catégorie",
                            labels={'total': 'Montant (USD)', 'category': 'Catégorie'},
                            color='total',
                            color_continuous_scale='Reds'
                        )
                        fig_anom1.update_xaxes(tickangle=45)
                        st.plotly_chart(fig_anom1, use_container_width=True)
                    
                    with col_anom5:
                        # Distribution des montants d'anomalies
                        fig_anom2 = px.histogram(
                            anomalies_df,
                            x='purchase_amount_usd',
                            nbins=30,
                            title="Distribution des Montants d'Anomalies",
                            labels={'purchase_amount_usd': 'Montant (USD)', 'count': 'Nombre'},
                            color_discrete_sequence=['red']
                        )
                        st.plotly_chart(fig_anom2, use_container_width=True)
                    
                    # Anomalies par localisation
                    if 'location' in anomalies_df.columns:
                        anom_by_location = anomalies_df.groupby('location')['purchase_amount_usd'].agg(['count', 'sum']).reset_index()
                        anom_by_location.columns = ['location', 'count', 'total']
                        anom_by_location = anom_by_location.sort_values('total', ascending=False).head(15)
                        
                        fig_anom3 = px.bar(
                            anom_by_location,
                            x='location',
                            y='count',
                            title="Nombre d'Anomalies par Localisation (Top 15)",
                            labels={'count': "Nombre d'anomalies", 'location': 'Localisation'},
                            color='count',
                            color_continuous_scale='Oranges'
                        )
                        fig_anom3.update_xaxes(tickangle=45)
                        st.plotly_chart(fig_anom3, use_container_width=True)
                    
                    st.markdown("### Tableau des Anomalies")
                    display_cols_anom = ['processed_time', 'customer_id', 'category', 'purchase_amount_usd',
                                        'amount_category', 'location', 'customer_segment', 'payment_method']
                    available_cols_anom = [col for col in display_cols_anom if col in anomalies_df.columns]
                    st.dataframe(anomalies_df[available_cols_anom], use_container_width=True, hide_index=True)
                else:
                    st.success("✅ Aucune anomalie détectée dans les données.")
            else:
                st.warning("La colonne is_anomaly n'est pas disponible.")
        
        with tab8:
            st.subheader("📊 Analyse Avancée - Segments & Satisfaction")
            
            # Revenu par segment client
            st.markdown("### 💰 Revenu Total par Segment Client")
            if 'customer_segment' in orders_df.columns:
                revenue_by_segment = orders_df.groupby('customer_segment').agg({
                    'purchase_amount_usd': ['sum', 'mean', 'count'],
                    'estimated_profit_usd': 'sum' if 'estimated_profit_usd' in orders_df.columns else 'count'
                }).reset_index()
                revenue_by_segment.columns = ['segment', 'total_revenue', 'avg_revenue', 'count', 'total_profit']
                revenue_by_segment = revenue_by_segment.sort_values('total_revenue', ascending=False)
                
                col_adv1, col_adv2 = st.columns(2)
                
                with col_adv1:
                    fig_seg1 = px.bar(
                        revenue_by_segment,
                        x='segment',
                        y='total_revenue',
                        title="Revenus Totaux par Segment",
                        labels={'total_revenue': 'Revenus (USD)', 'segment': 'Segment Client'},
                        color='total_revenue',
                        color_continuous_scale='Blues'
                    )
                    st.plotly_chart(fig_seg1, use_container_width=True)
                
                with col_adv2:
                    fig_seg2 = px.bar(
                        revenue_by_segment,
                        x='segment',
                        y='count',
                        title="Nombre de Commandes par Segment",
                        labels={'count': 'Nombre de commandes', 'segment': 'Segment Client'},
                        color='segment',
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    st.plotly_chart(fig_seg2, use_container_width=True)
                
                st.dataframe(revenue_by_segment, use_container_width=True, hide_index=True)
            else:
                st.warning("La colonne customer_segment n'est pas disponible.")
            
            st.markdown("---")
            
            # Satisfaction par catégorie de produit
            st.markdown("### 😊 Analyse de Satisfaction par Catégorie")
            if 'satisfaction_level' in orders_df.columns and 'category' in orders_df.columns:
                satisfaction_by_category = orders_df.groupby(['category', 'satisfaction_level']).agg({
                    'purchase_amount_usd': ['count', 'sum']
                }).reset_index()
                satisfaction_by_category.columns = ['category', 'satisfaction_level', 'count', 'revenue']
                
                col_adv3, col_adv4 = st.columns(2)
                
                with col_adv3:
                    # Heatmap satisfaction par catégorie
                    pivot_satisfaction = satisfaction_by_category.pivot_table(
                        index='category',
                        columns='satisfaction_level',
                        values='count',
                        aggfunc='sum',
                        fill_value=0
                    )
                    
                    # Trier les niveaux de satisfaction dans un ordre logique
                    satisfaction_order = ['Very Satisfied', 'Satisfied', 'Neutral', 'Dissatisfied']
                    available_levels = [level for level in satisfaction_order if level in pivot_satisfaction.columns]
                    if available_levels:
                        pivot_satisfaction = pivot_satisfaction[available_levels]
                    
                    fig_sat1 = px.imshow(
                        pivot_satisfaction,
                        labels=dict(x="Niveau de Satisfaction", y="Catégorie", color="Nombre de commandes"),
                        title="Heatmap: Satisfaction par Catégorie",
                        color_continuous_scale='RdYlGn',
                        aspect="auto"
                    )
                    st.plotly_chart(fig_sat1, use_container_width=True)
                
                with col_adv4:
                    # Répartition de la satisfaction globale
                    satisfaction_dist = orders_df['satisfaction_level'].value_counts()
                    fig_sat2 = px.pie(
                        values=satisfaction_dist.values,
                        names=satisfaction_dist.index,
                        title="Répartition Globale de la Satisfaction",
                        hole=0.4,
                        color_discrete_sequence=px.colors.sequential.RdYlGn
                    )
                    st.plotly_chart(fig_sat2, use_container_width=True)
                
                # Graphique en barres groupées
                fig_sat3 = px.bar(
                    satisfaction_by_category,
                    x='category',
                    y='count',
                    color='satisfaction_level',
                    title="Nombre de Commandes par Catégorie et Niveau de Satisfaction",
                    labels={'count': 'Nombre de commandes', 'category': 'Catégorie', 'satisfaction_level': 'Satisfaction'},
                    barmode='group'
                )
                fig_sat3.update_xaxes(tickangle=45)
                st.plotly_chart(fig_sat3, use_container_width=True)
                
                st.dataframe(satisfaction_by_category.sort_values('count', ascending=False), use_container_width=True, hide_index=True)
            else:
                st.warning("Les colonnes satisfaction_level et category ne sont pas disponibles.")
            
            st.markdown("---")
            
            # Analyses supplémentaires
            st.markdown("### 📈 Analyses Supplémentaires")
            
            col_adv5, col_adv6 = st.columns(2)
            
            with col_adv5:
                if 'amount_category' in orders_df.columns:
                    st.markdown("#### Répartition par Catégorie de Montant")
                    amount_cat_dist = orders_df['amount_category'].value_counts()
                    fig_amt = px.bar(
                        x=amount_cat_dist.index,
                        y=amount_cat_dist.values,
                        title="Répartition des Commandes par Catégorie de Montant",
                        labels={'x': 'Catégorie de Montant', 'y': 'Nombre de commandes'},
                        color=amount_cat_dist.values,
                        color_continuous_scale='Viridis'
                    )
                    st.plotly_chart(fig_amt, use_container_width=True)
            
            with col_adv6:
                if 'frequency_category' in orders_df.columns:
                    st.markdown("#### Répartition par Fréquence")
                    freq_dist = orders_df['frequency_category'].value_counts()
                    fig_freq = px.pie(
                        values=freq_dist.values,
                        names=freq_dist.index,
                        title="Répartition par Catégorie de Fréquence",
                        hole=0.4
                    )
                    st.plotly_chart(fig_freq, use_container_width=True)
        
        # Table des dernières commandes
        st.markdown("---")
        st.subheader("📋 Dernières commandes (streaming)")
        
        # Filtres
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            categories_filter = st.multiselect(
                "Filtrer par catégorie",
                options=orders_df['category'].unique() if 'category' in orders_df.columns else [],
                default=[]
            )
        with col_f2:
            locations_filter = st.multiselect(
                "Filtrer par localisation",
                options=orders_df['location'].unique() if 'location' in orders_df.columns else [],
                default=[]
            )
        with col_f3:
            limit_display = st.slider("Nombre de lignes à afficher", 10, 500, 100)
        
        # Appliquer les filtres
        filtered_df = orders_df.copy()
        if categories_filter:
            filtered_df = filtered_df[filtered_df['category'].isin(categories_filter)]
        if locations_filter:
            filtered_df = filtered_df[filtered_df['location'].isin(locations_filter)]
        
        # Afficher le tableau (inclure les nouvelles colonnes enrichies)
        display_columns = ['processed_time', 'customer_id', 'category', 'item_purchased', 
                          'purchase_amount_usd', 'final_amount_usd', 'amount_category',
                          'customer_segment', 'satisfaction_level', 'is_anomaly', 
                          'location', 'review_rating']
        available_columns = [col for col in display_columns if col in filtered_df.columns]
        
        st.dataframe(
            filtered_df[available_columns].head(limit_display),
            use_container_width=True,
            hide_index=True
        )
        
        st.caption(f"Affiche {min(limit_display, len(filtered_df))} lignes sur {len(filtered_df)} total")

except Exception as e:
    st.error(f"Erreur lors de la récupération des données: {str(e)}")
    st.exception(e)
    st.info("💡 Vérifiez votre configuration BigQuery et vos credentials GCP")

# Actualisation automatique
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()


