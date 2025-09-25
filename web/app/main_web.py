import streamlit as st

# 1. Coloque a configuração da página no início do script
st.set_page_config(
    page_title="Solon AI - Home",
    layout="wide"
)

# 2. Defina o conteúdo da sua página principal em uma função
def home():
    st.title("Bem-vindo ao Solon AI Analytics")
    st.markdown("""
    Esta plataforma é dedicada à análise de dados do mercado financeiro, 
    fornecendo insights sobre setores e empresas.

    **Use a barra lateral para navegar entre as páginas de análise:**
    - **Análise de Setores:** Visualize o score de sentimento para diferentes setores da economia.
    - **Análise de Empresas:** Acompanhe o desempenho e o score de empresas específicas.
    """)

# 3. Crie objetos "Page" para cada uma das suas páginas
home_page = st.Page(home, title="Home", icon=":material/home:", default=True)
setores_page = st.Page("pages/1_setores.py", title="Análise de Setores", icon=":material/insights:")
empresas_page = st.Page("pages/2_empresas.py", title="Análise de Empresas", icon=":material/business:")

# 4. Use st.navigation para construir a barra lateral com as páginas definidas
pg = st.navigation([home_page, setores_page, empresas_page])

# 5. Execute a navegação
pg.run()