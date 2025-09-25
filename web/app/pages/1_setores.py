import streamlit as st
import pandas as pd
import altair as alt
from urllib.error import URLError
import requests

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

        /* Força a cor do texto para branco em todo o app */
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

st.markdown("# Solon AI - Análise de Setores")
st.write("Demo com controle de datas, estilo customizado e gráfico centralizado.")

@st.cache_data
def get_setores():
    SETORES_URL = 'http://localhost:8000/setores'
    setores = requests.get(SETORES_URL).json()
    df = pd.DataFrame(setores)
    return df.set_index('setores')

@st.cache_data
def get_datas(setor, perfil, tipo):
    SETORES_URL = f'http://localhost:8000/{setor}?perfil={perfil}&tipo={tipo}'
    dados_setores = requests.get(SETORES_URL).json()
    df = pd.DataFrame(dados_setores["data"])
    df["setor"] = setor
    df["periodo"] = pd.to_datetime(df["periodo"])
    return df

try:
    df = get_setores()
    
    setores = st.multiselect(
        'Escolha os Setores', list(df.index), ['Agricultura']
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
        
    if not setores:
        st.error('Por favor, selecione pelo menos um setor!')
    elif not perfil:
        st.warning('Por favor, selecione um perfil de investidor para continuar.')
    else:     
        data = pd.concat([get_datas(setor, perfil, tipo) for setor in setores])
        data["score"] = data["score"].round(2)

        min_date, max_date = data["periodo"].min(), data["periodo"].max()
        extra_days = 0.2

        if tipo == "Mensal":
            x_field = "periodo:T"
            x_format = "%b/%Y"
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
                    scale=alt.Scale(domain=[min_date, max_date + pd.Timedelta(days=extra_days)])
                ),
                y=alt.Y(
                    "score:Q",
                    title="Score",
                    axis=alt.Axis(grid=True, gridColor="#444444"),
                    scale=alt.Scale(domain=[0, 5])
                ),
                color=alt.Color(
                    "setor:N",
                    scale=alt.Scale(scheme='pastel1'),
                    legend=alt.Legend(title="Setores", orient="bottom")
                ),
                tooltip=["periodo:T", "score:Q", "setor:N"]
            )
        )
        
        chart = (
            line_chart
            .interactive()
            .properties(
                title=alt.TitleParams(
                    text=f'Score {tipo.capitalize()} por Setor',
                    anchor='middle',
                    color='white'
                ),
                height=500,
                background="#1c1f26",
            )
        )

        chart_col1, chart_col2, chart_col3 = st.columns([0.1, 0.8, 0.1])
        with chart_col2:
            st.altair_chart(chart, use_container_width=True)

except URLError as e:
    st.error(f"Erro de conexão: {e.reason}")
except requests.exceptions.ConnectionError as e:
    st.error(f"Não foi possível conectar à API. Verifique se o servidor local está rodando em http://localhost:8000. Erro: {e}")
