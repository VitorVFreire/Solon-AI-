import streamlit as st
import pandas as pd
import altair as alt
from urllib.error import URLError
import requests

st.set_page_config(page_title="Solon AI - Empresas", layout="wide")

st.markdown(
    """
    <style>
        body {
            background-color: #0e1117;
            color: white;
            font-family: "Arial", sans-serif;
            font-size: 16px;
            line-height: 1.6;
        }
        [data-testid="stAppViewContainer"] *,
        [data-testid="stLayoutWrapper"] *,
        [data-testid="stSidebar"] * {
            color: white;
        }
        .stApp {
            background-color: #0e1117;
        }
        section[data-testid="stSidebar"] {
            background-color: #1c1f26;
        }
        div[data-baseweb="select"] {
            background-color: #2c3038;
            color: white;
        }
        h1, h2, h3, h4 {
            text-align: center;
        }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("# Solon AI - Análise de Empresas")
st.write("Selecione uma ou mais empresas, o perfil de investidor e a granularidade para visualizar a evolução do score.")

@st.cache_data

def get_companies():
    URL = 'http://localhost:8000/companies'
    companies = requests.get(URL).json()
    df = pd.DataFrame(companies)
    return df.set_index('companies')

@st.cache_data
def get_datas(company, perfil, tipo):
    URL = f'http://localhost:8000/company/{company}?perfil={perfil}&tipo={tipo}'
    response = requests.get(URL)
    response.raise_for_status()
    dados_company = response.json()
    df = pd.DataFrame(dados_company["data"])
    if not df.empty:
        df["company"] = company
        df["periodo"] = pd.to_datetime(df["periodo"])
    return df

try:
    df_companies = get_companies()
    
    companies = st.multiselect(
        'Escolha as Empresas', list(df_companies.index)
    )

    col1, col2 = st.columns(2, gap="large")

    with col1:
        perfil = st.radio(
            "Selecione o seu perfil de investidor:",
            ('Conservador', 'Moderado', 'Agressivo'),
            index=0,
            horizontal=True
        )

    with col2:
        tipo = st.radio(
            "Selecione a granularidade:",
            ("Diario", "Mensal"),
            index=0,
            horizontal=True
        )
        
    if not companies:
        st.error('Por favor, selecione pelo menos uma empresa!')
    else:
        dataframes = [get_datas(company, perfil, tipo) for company in companies]
        data = pd.concat(dataframes, ignore_index=True)

        if data.empty:
            st.warning("Nenhum dado encontrado para as empresas selecionadas com os filtros aplicados.")
        else:
            data["score"] = data["score"].round(2)

            min_date, max_date = data["periodo"].min(), data["periodo"].max()

            if tipo == "Mensal":
                x_field = "yearmonth(periodo):T"
                x_format = "%m/%Y"
                tick_values = pd.date_range(start=min_date, end=max_date, freq='MS').to_pydatetime()
            else:
                x_field = "periodo:T"
                x_format = "%d/%m"
                tick_values = pd.date_range(start=min_date, end=max_date).to_pydatetime()

            line_chart = (
                alt.Chart(data)
                .mark_line(point=True)
                .encode(
                    x=alt.X(
                        x_field,
                        axis=alt.Axis(
                            format=x_format,
                            title="Data",
                            grid=True,
                            gridColor="#444444",
                            values=list(tick_values)
                        ),
                        scale=alt.Scale(domain=[min_date.to_pydatetime(), max_date.to_pydatetime()])
                    ),
                    y=alt.Y(
                        "score:Q",
                        title="Score",
                        axis=alt.Axis(grid=True, gridColor="#444444"),
                        scale=alt.Scale(domain=[0, 5])
                    ),
                    color=alt.Color(
                        "company:N",
                        scale=alt.Scale(scheme='pastel1'),
                        legend=alt.Legend(title="Empresas", orient="bottom")
                    ),
                    tooltip=["periodo:T", "score:Q", "company:N"]
                )
            )
            
            chart = (
                line_chart
                .interactive()
                .properties(
                    title=alt.TitleParams(
                        text=f'Score {tipo} por Empresa',
                        anchor='middle',
                        color='white'
                    ),
                    height=500,
                    background="#1c1f26",
                )
                .configure_axis(
                    labelColor='white',
                    titleColor='white'
                )
                .configure_legend(
                    labelColor='white',
                    titleColor='white'
                )
            )

            st.altair_chart(chart, use_container_width=True)

except URLError as e:
    st.error(f"Erro de conexão (URL): {e.reason}")
except requests.exceptions.ConnectionError:
    st.error("Não foi possível conectar à API. Verifique se o servidor local está rodando em http://localhost:8000.")
except Exception as e:
    st.error(f"Ocorreu um erro inesperado: {e}")