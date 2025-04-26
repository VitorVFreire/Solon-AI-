def calcular_dv_cnpj(cnpj_parcial):
    """Calcula os dois dígitos verificadores do CNPJ (recebe 12 dígitos)."""
    def calcular_digito(cnpj, pesos):
        soma = sum(int(digito) * peso for digito, peso in zip(cnpj, pesos))
        resto = soma % 11
        return '0' if resto < 2 else str(11 - resto)

    pesos_primeiro = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos_segundo = [6] + pesos_primeiro

    d1 = calcular_digito(cnpj_parcial, pesos_primeiro)
    d2 = calcular_digito(cnpj_parcial + d1, pesos_segundo)

    return d1 + d2

def gerar_cnpj_completo(cnpj_basico):
    """Gera o CNPJ completo formatado com base nos 8 dígitos iniciais."""
    if len(cnpj_basico) != 8 or not cnpj_basico.isdigit():
        raise ValueError("CNPJ básico deve conter 8 dígitos numéricos.")
    
    numero_ordem = "0001"  # Matriz
    cnpj_sem_dv = cnpj_basico + numero_ordem
    dv = calcular_dv_cnpj(cnpj_sem_dv)
    cnpj_completo = cnpj_sem_dv + dv

    # Formatação padrão: 00.000.000/0001-00
    formatado = f"{cnpj_completo[:2]}{cnpj_completo[2:5]}{cnpj_completo[5:8]}{cnpj_completo[8:12]}{cnpj_completo[12:]}"
    return formatado