import streamlit as st

st.set_page_config(
    page_title="Solon AI - Home",
    layout="wide"
)

def home():
    st.title("Bem-vindo ao Solon AI Analytics")
    st.markdown("""
    Esta plataforma é dedicada à análise de dados do mercado financeiro, 
    fornecendo insights sobre setores e empresas.

    **Use a barra lateral para navegar entre as páginas de análise:**
    - **Análise de Setores:** Visualize o score de sentimento para diferentes setores da economia.
    - **Análise de Empresas:** Acompanhe o desempenho e o score de empresas específicas.
    """)

home_page = st.Page(home, title="Home", icon=":material/home:", default=True)
setores_page = st.Page("pages/1_setores.py", title="Análise de Setores", icon=":material/insights:")
empresas_page = st.Page("pages/2_empresas.py", title="Análise de Empresas", icon=":material/business:")
empresa_page = st.Page("pages/3_empresa.py", title="Análise de Empresa", icon=":material/business:")

pg = st.navigation([home_page, setores_page, empresas_page,empresa_page])

pg.run()