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
            if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
                credentials = service_account.Credentials.from_service_account_info(
                    st.secrets['gcp_service_account'],
                    scopes=["https://www.googleapis.com/auth/bigquery"]
                )
                client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
                return client
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
        
        if "403" in error_msg or "401" in error_msg or "CREDENTIALS" in error_msg:
            st.error("**Problème d'authentification détecté**")
            st.markdown("""
            **Solutions possibles :**
            
            1. **Configurer Application Default Credentials :**
               ```powershell
               gcloud auth application-default login
               gcloud config set project spark-streaming-483317
               ```
            
            2. **OU utiliser un fichier Service Account :**
               - Téléchargez un fichier JSON de Service Account depuis GCP Console
               - Définissez la variable d'environnement :
                 ```powershell
                 $env:GOOGLE_APPLICATION_CREDENTIALS="C:\\chemin\\vers\\service-account-key.json"
                 ```
            
            3. **Vérifier que le projet est actif :**
               ```powershell
               gcloud config get-value project
               gcloud projects describe spark-streaming-483317
               ```
            """)
        raise

@st.cache_data(ttl=10)  # Cache pendant 10 secondes pour réduire les appels BigQuery
def fetch_latest_orders(limit=1000):
    """Récupère les dernières commandes depuis BigQuery"""
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
        processed_time
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    ORDER BY processed_time DESC
    LIMIT {limit}
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Erreur lors de la récupération des commandes: {str(e)}")
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
        col1, col2, col3, col4 = st.columns(4)
        
        total_orders = len(orders_df)
        total_revenue = orders_df['purchase_amount_usd'].sum()
        avg_order_value = orders_df['purchase_amount_usd'].mean()
        avg_rating = orders_df['review_rating'].mean()
        
        col1.metric("Total Commandes", f"{total_orders:,}")
        col2.metric("Revenus Total", f"${total_revenue:,.2f}")
        col3.metric("Panier Moyen", f"${avg_order_value:.2f}")
        col4.metric("Note Moyenne", f"{avg_rating:.2f}")
        
        # Graphiques dans des onglets
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📈 Vue d'ensemble", 
            "👥 Par Âge", 
            "⚧️ Par Genre", 
            "🌍 Par Localisation",
            "🔀 Combinaisons"
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
        
        # Afficher le tableau
        display_columns = ['processed_time', 'customer_id', 'category', 'item_purchased', 
                          'purchase_amount_usd', 'location', 'review_rating']
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


