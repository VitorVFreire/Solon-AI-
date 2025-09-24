import streamlit as st
import pandas as pd
import altair as alt
from urllib.error import URLError
import requests
import time

st.set_page_config(page_title="Solon AI - Setores", layout="wide")

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

st.markdown("# Solon AI - Setores")
st.sidebar.header("Filtros Principais")
st.write("Demo com controle de datas, estilo customizado e gráfico centralizado.")

@st.cache_data
def get_setores():
    SETORES_URL = 'http://localhost:8000/setores'
    setores = requests.get(SETORES_URL).json()
    df = pd.DataFrame(setores)
    return df.set_index('setores')

@st.cache_data
def get_datas(setor, perfil):
    SETORES_URL = f'http://localhost:8000/candlestick/{setor}?perfil={perfil}'
    dados_setores = requests.get(SETORES_URL).json()
    df = pd.DataFrame(dados_setores["data"])
    df["setor"] = setor
    df["dia"] = pd.to_datetime(df["dia"])
    return df

try:
    df = get_setores()
    
    setores = st.multiselect(
        'Escolha os Setores', list(df.index), ['Agricultura']
    )

    perfil = st.radio(
        "Selecione o seu perfil de investidor:",
        ('Conservador', 'Moderado', 'Agressivo'),
        index=None,
        horizontal=True
    )

    if not setores:
        st.error('Por favor, selecione pelo menos um setor!')
    elif not perfil:
        st.warning('Por favor, selecione um perfil de investidor para continuar.')
    else:
        success_message = st.success(f"Perfil de investidor selecionado: **{perfil}**")
        time.sleep(2)
        success_message.empty()
        
        data = pd.concat([get_datas(setor, perfil) for setor in setores])
        data["daily_score"] = data["daily_score"].round(2)

        min_date, max_date = data["dia"].min(), data["dia"].max()
        extra_days = 0.2

        tick_values = pd.date_range(start=min_date, end=max_date).to_pydatetime()

        line_chart = (
            alt.Chart(data)
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    "dia:T",
                    axis=alt.Axis(
                        format="%d/%m", 
                        title="Data", 
                        grid=True, 
                        gridColor="#444444",
                        values=list(tick_values)
                    ),
                    scale=alt.Scale(domain=[min_date, max_date + pd.Timedelta(days=extra_days)])
                ),
                y=alt.Y(
                    "daily_score:Q",
                    title="Score",
                    axis=alt.Axis(grid=True, gridColor="#444444"),
                    scale=alt.Scale(domain=[0, 5])
                ),
                color=alt.Color(
                    "setor:N",
                    scale=alt.Scale(scheme='pastel1'),
                    legend=alt.Legend(title="Setores", orient="bottom")
                ),
                tooltip=["dia:T", "daily_score:Q", "setor:N"]
            )
        )
        
        chart = (
            line_chart
            .interactive()
            .properties(
                title=alt.TitleParams(text='Score Diário por Setor', anchor='middle', color='white'),
                height=500,
                background="#1c1f26",
            )
        )

        col1, col2, col3 = st.columns([0.1, 0.8, 0.1])
        with col2:
            st.altair_chart(chart, use_container_width=True)

except URLError as e:
    st.error(f"Erro de conexão: {e.reason}")
except requests.exceptions.ConnectionError as e:
    st.error(f"Não foi possível conectar à API. Verifique se o servidor local está rodando em http://localhost:8000. Erro: {e}")
