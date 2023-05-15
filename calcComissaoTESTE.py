import config
import pandas as pd
from hdbcli import dbapi as db
from decimal import Decimal
import datetime
import json

# Function to print json objects with decimals
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

#------------------ DATABASE CONNECTION -------------------
SAPcredenciais = config.credentials["HANA"]
SAPconexao = db.connect(
    address = SAPcredenciais["address"],
    port = SAPcredenciais["port"],
    user = SAPcredenciais["user"],
    password = SAPcredenciais["password"],
)

# --------------- QUERY PRODUTOS --------------------
def queryProdutos(colunaNome, conexaoBD):
    data = conexaoBD.cursor()

    colunaValores = []
    sqlPRD = f"""
        SELECT 
        lp.\"{colunaNome}\"  
        FROM 
        SBO_SITIO_PRD.LISTA_PRODUTOS lp 
        WHERE lp.\"Codigo_Item\" LIKE 'PA%'
    """

    data.execute(sqlPRD)

    for row in data:
        colunaValores.append(row[0])
    data.close()    
    
    return colunaValores

# --------------- QUERY ROTA --------------------
def queryRotas(conexaoBD):
    data = conexaoBD.cursor()

    rotas = []
    sqlPRD = f"""
        SELECT 
        nr."Code" 
        FROM 
        SBO_SITIO_PRD."@NXT_ROTA" nr 
        WHERE 
        nr."U_NXTDescricao" = 'Atacado' 
        OR nr."U_NXTDescricao" = 'Grandes Redes'
    """

    data.execute(sqlPRD)

    for row in data:
        rotas.append(row[0])
    data.close()    
    
    return rotas

# --------------- MAIN QUERY VENDAS --------------------
def queryVendas(colunaNome, timestamp, rota, conexaoBD):
    data = conexaoBD.cursor()
    colunaValores = []
    mes = timestamp.month
    mesAnt = (timestamp - datetime.timedelta(days=27)).month
    ano = timestamp.year
    anoAnt = (timestamp - datetime.timedelta(days=27)).year
    
    sqlPRD = f"""
        SELECT
        w."{colunaNome}"
        FROM
        (
            SELECT
            *
            FROM
            SBO_SITIO_PRD.NEXT_APRESENTACAO_VENDAS v
            WHERE
            v.\"Data de lançamento\" >= '{anoAnt}-{mesAnt}-26'
            AND v.\"Data de lançamento\" <= '{ano}-{mes}-25'
            AND v.\"Rota\" = '{rota}'
        ) w
        WHERE
        w. \"Nº do item\" LIKE 'PA%'
        OR w.\"Nº do item\" IS NULL
    """

    data.execute(sqlPRD)

    for row in data:
        colunaValores.append(row[0])
    data.close()    
    
    return colunaValores

# --------------- QUERY DESCONTOS --------------------
def queryDescontos(colunaNome, timestamp, conexaoBD):
    data = conexaoBD.cursor()
    mes = timestamp.month
    ano = timestamp.year
    colunaValores = []
    
    sqlPRD = f"""
        SELECT
        dl."{colunaNome}"
        FROM
        SBO_SITIO_TESTE."@COM_DESCFIN_L\" dl
        LEFT JOIN SBO_SITIO_TESTE."@COM_DESCFIN_C\" dc ON
        dl.\"Code\" = dc.\"Code\"
        WHERE dc.\"U_DESC_DATA_FINAL\" >= '{ano}-{mes}-25'
    """

    data.execute(sqlPRD)

    for row in data:
        colunaValores.append(row[0])
    data.close()

    return colunaValores

# --------------- QUERY META --------------------
def queryMeta(colunaNome, timestamp, conexaoBD):
    data = conexaoBD.cursor()
    
    colunaValores = []
    mes = timestamp.month
    mesAnt = (timestamp - datetime.timedelta(days=27)).month
    ano = timestamp.year
    anoAnt = (timestamp - datetime.timedelta(days=27)).year
    
    sqlPRD = f"""
        SELECT
        ml.\"{colunaNome}"
        FROM
        SBO_SITIO_TESTE.\"@NXT_METAL\" ml
        LEFT JOIN SBO_SITIO_TESTE.\"@NXT_METAC\" mc ON
        ml.\"DocEntry\" = mc.\"DocEntry\"
        WHERE
        mc.\"U_NXT_DataInicio\" >= '{anoAnt}-{mesAnt}-26'
        AND mc.\"U_NXT_DataFim\" <= '{ano}-{mes}-25'
    """

    data.execute(sqlPRD)

    for row in data:
        colunaValores.append(row[0])
    data.close()

    return colunaValores

# --------------- QUERY COMISSAO --------------------
def queryComissao(colunaNome, timestamp, conexaoBD):
    data = conexaoBD.cursor()
    colunaValores = []
    mes = timestamp.month
    mesAnt = (timestamp - datetime.timedelta(days=27)).month
    ano = timestamp.year
    anoAnt = (timestamp - datetime.timedelta(days=27)).year
    
    sqlPRD = f"""
        SELECT
        cl.\"{colunaNome}"
        FROM
        SBO_SITIO_TESTE.\"@COM_COMISSAO_L\" cl
        LEFT JOIN SBO_SITIO_TESTE.\"@COM_COMISSAO_C\" cc ON
        cc.\"DocEntry\" = cl.\"DocEntry\"
        WHERE
        cc.\"U_COMISS_DATA_INICIAL\" >= '{anoAnt}-{mesAnt}-26' 
        AND cc.\"U_COMISS_DATA_FINAL\" <= '{ano}-{mes}-25'
    """

    data.execute(sqlPRD)

    for row in data:
        colunaValores.append(row[0])
    data.close()
    return colunaValores

#------------------ VARIAVEIS INPUTAVEIS ------------------
## Data
timestampAtual = datetime.date.today()
# timestampAtual = timestampAtual - datetime.timedelta(days=61)
if timestampAtual.day >= 26:
    timestampAtual = timestampAtual + datetime.timedelta(days=20)
print(timestampAtual)
## Rota desejada
rotas = queryRotas(SAPconexao)


def calculoComissao(timestamp, rotaDesejada):
    #------------------ DESCONTOS P/ REDE ------------------
    descontos = {
        "grupo": queryDescontos("U_Rede", timestamp, SAPconexao),
        "cliente": queryDescontos("U_Cliente", timestamp, SAPconexao),
        "desc": queryDescontos("U_TaxaDesconto", timestamp, SAPconexao),
    }

    faixasComissoes = {
        "Vendedor": queryComissao("U_COMISS_NOME_VEND", timestamp, SAPconexao),
        "Rota": queryComissao("U_COMISS_ROTA", timestamp, SAPconexao),
        "Faixa 1%": queryComissao("U_COMISS_FAIXA1", timestamp, SAPconexao),
        "Comissão 1%": queryComissao("U_COMISS_PORC1", timestamp, SAPconexao),
        "Faixa 2%": queryComissao("U_COMISS_FAIXA2", timestamp, SAPconexao),
        "Comissão 2%": queryComissao("U_COMISS_PORC2", timestamp, SAPconexao),
        "Faixa 3%": queryComissao("U_COMISS_FAIXA3", timestamp, SAPconexao),
        "Comissão 3%": queryComissao("U_COMISS_PORC3", timestamp, SAPconexao),
    }

    for i in range(len(faixasComissoes)):
        faixasComissoes["Comissão 1%"][i] = faixasComissoes["Comissão 1%"][i] / 10
        faixasComissoes["Comissão 2%"][i] = faixasComissoes["Comissão 2%"][i] / 10
        faixasComissoes["Comissão 3%"][i] = faixasComissoes["Comissão 3%"][i] / 10
        faixasComissoes["Faixa 1%"][i] = faixasComissoes["Faixa 1%"][i] / 100
        faixasComissoes["Faixa 2%"][i] = faixasComissoes["Faixa 2%"][i] / 100
        faixasComissoes["Faixa 3%"][i] = faixasComissoes["Faixa 3%"][i] / 100
        # faixasComissoes["Comissão 1%"][i] = faixasComissoes["Comissão 1%"][i] / 100
        # faixasComissoes["Comissão 2%"][i] = faixasComissoes["Comissão 2%"][i] / 100
        # faixasComissoes["Comissão 3%"][i] = faixasComissoes["Comissão 3%"][i] / 100
    faixasComissoes = pd.DataFrame(faixasComissoes)
    faixasComissoes = faixasComissoes.loc[faixasComissoes["Rota"] == rotaDesejada]
    #------------------ META p/ VENDEDOR ------------------
    meta = {
        "codigo": queryMeta("U_NXT_Item", timestamp, SAPconexao),
        "produto": queryMeta("U_NXT_Name", timestamp, SAPconexao),
        "grupo": queryMeta("U_NXT_Grp", timestamp, SAPconexao),
        "marca": queryMeta("U_NXT_Firm", timestamp, SAPconexao),
        "medida": queryMeta("U_NXT_UND", timestamp, SAPconexao),
        "vendedor": queryMeta("U_NXT_Slp", timestamp, SAPconexao),
        "rota": queryMeta("U_NXT_Rota", timestamp, SAPconexao),
        "qt": queryMeta("U_NXT_QTD", timestamp, SAPconexao),
        "tipoComissao": queryMeta("U_NXT_TipoCom", timestamp, SAPconexao),
    }
    meta = pd.DataFrame(meta)
    meta = meta.loc[meta["rota"] == rotaDesejada]
    # print(meta)

    produtos = {
        "cod": queryProdutos("Codigo_Item", SAPconexao),
        "desc": queryProdutos("Nome_Item", SAPconexao),
        "grupo": queryProdutos("Grupo_de_Produto", SAPconexao),
        "marca": queryProdutos("Marca", SAPconexao),
    }
    # print(produtos)
        
    #------------------ IMPORTANT COLUMNS ------------------
    sold = {
        "rota": queryVendas("Rota", timestamp, rotaDesejada, SAPconexao),
        "produto": queryVendas(r"Descrição do item/serviço",  timestamp, rotaDesejada, SAPconexao),
        "codigo": queryVendas("Nº do item", timestamp, rotaDesejada, SAPconexao),    
        "vendedor": queryVendas("Vendedor", timestamp, rotaDesejada, SAPconexao),    
        "grupo": queryVendas("Grupo de itens", timestamp, rotaDesejada, SAPconexao),
        "grupoCliente": queryVendas("Grupo cliente", timestamp, rotaDesejada, SAPconexao),
        "qtdAtingidaKG": queryVendas("Em KG", timestamp, rotaDesejada, SAPconexao),
        "vlrFaturado": queryVendas("Vlr.Faturado", timestamp, rotaDesejada, SAPconexao),
        "volDevolvido": queryVendas("Qtd.Devolvida", timestamp, rotaDesejada, SAPconexao),
        "vlrDevolvido": queryVendas("Vlr.Devolvido", timestamp, rotaDesejada, SAPconexao),
        "medida": queryVendas("Código da UM", timestamp, rotaDesejada, SAPconexao),
        "tipoNota": queryVendas("Utilização", timestamp, rotaDesejada, SAPconexao),
        "responsabilidade": queryVendas("Responsável Devolução", timestamp, rotaDesejada, SAPconexao),
        "motivoDevolucao": queryVendas("Motivo da Devolução", timestamp, rotaDesejada, SAPconexao),
        "doc": queryVendas("Documento", timestamp, rotaDesejada, SAPconexao),
        "data": queryVendas("Data de lançamento", timestamp, rotaDesejada, SAPconexao),
    }
    # print(pd.DataFrame(sold))

    # Valores faturados por produtos
    vlrsFaturados = {}

    # Listagem de produtos
    lsProdutos = produtos["cod"]
    # print(lsProdutos)
    # # Listagem de grupo produtos
    lsGrupoProduto = {
        "cod": [],
        "nome": [],
        "grupo": [],
    }
    for prd in range(len(produtos["cod"])):
        lsGrupoProduto["cod"].append(f'{produtos["cod"][prd]}')
        lsGrupoProduto["nome"].append(f'{produtos["desc"][prd]}')
        if ({produtos["grupo"][prd]} == {produtos["marca"][prd]}):
            lsGrupoProduto["grupo"].append(f'{produtos["grupo"][prd]}')
        else:
            lsGrupoProduto["grupo"].append(f'{produtos["grupo"][prd]} {produtos["marca"][prd]}')
    # lsProdutos.extend(lsGrupoProduto["grupo"])
    # print(lsProdutos)

    lsProdutos = pd.DataFrame(produtos["cod"]).dropna().drop_duplicates().values.tolist()
    lsGrupoProduto = pd.DataFrame(lsGrupoProduto)
    # print(pd.DataFrame(lsGrupoProduto))

#--------------- DECLARACAO DE COLUNAS TABELA FINAL --------------------------    
    for prd in range(len(lsProdutos)):
        vlrsFaturados[lsProdutos[prd][0]] = {
            "Produto": lsGrupoProduto["nome"].loc[lsGrupoProduto["cod"] == lsProdutos[prd][0]].values.item(),
            "Grupo Produto": lsGrupoProduto["grupo"].loc[lsGrupoProduto["cod"] == lsProdutos[prd][0]].values.item(),
            "Tipo Comissao": meta["tipoComissao"].loc[lsProdutos[prd][0] == meta["codigo"]].values.item() if len(meta["tipoComissao"].loc[lsProdutos[prd][0] == meta["codigo"]].values) > 0 else "SKU",
            "Data": timestamp,
            "Rota": rotaDesejada,
            "Vendedor": meta["vendedor"].loc[meta["codigo"] == lsProdutos[prd][0]].values.item() if len(meta["vendedor"].loc[meta["codigo"] == lsProdutos[prd][0]].values) > 0 else "",
            "Meta": meta["qt"].loc[meta["codigo"] == lsProdutos[prd][0]].values.item() if len(meta["qt"].loc[meta["codigo"] == lsProdutos[prd][0]]) > 0 else 0,
            "Volume": 0,
            "Volume Devolvido": 0,
            "Volume Bonificado": 0,
            "Valor Faturado R$": 0,
            "Valor Devolvido R$": 0,
            "Valor Bonificado R$": 0,  
            "Desconto Financeiro R$": 0,
            # "Desconto Acordo Comercial": 0
        }
        if (vlrsFaturados[lsProdutos[prd][0]]["Tipo Comissao"]) == "":
            vlrsFaturados[lsProdutos[prd][0]]["Tipo Comissao"] = "SKU"
    # print(vlrsFaturados)

    ## Soma total de devolucoes comerciais
    devolucoesComerciais = 0
    ## Soma total de acordos comerciais
    descAcordosComerciais = 0
    
    print(pd.DataFrame(vlrsFaturados))

    for prd in range(len(sold["produto"])):
        if sold["qtdAtingidaKG"][prd] != None:
            ## Vendedor
            vlrsFaturados[sold["codigo"][prd]]["Vendedor"] = sold["vendedor"][prd]
            ## Volume
            vlrsFaturados[sold["codigo"][prd]]["Volume"] += sold["qtdAtingidaKG"][prd]
            # vlrsFaturados[sold["codigo"][prd]]["Volume"][1] = sold["medida"][prd]
            ## Meta
            ## =--=--=- MODIFICAR META URGENTEMENTE PARA SAP =--=--=-
            vlrsFaturados[sold["codigo"][prd]]["Meta"] = meta["qt"].loc[meta["codigo"] == sold["codigo"][prd]].values
            if len(vlrsFaturados[sold["codigo"][prd]]["Meta"]) == 0:
                vlrsFaturados[sold["codigo"][prd]]["Meta"] = 0
            else:
                vlrsFaturados[sold["codigo"][prd]]["Meta"] = vlrsFaturados[sold["codigo"][prd]]["Meta"].item()
            # # vlrsFaturados[sold["codigo"][prd]]["Meta"][1] = sold["medida"][prd]
            ## Faturado
            vlrsFaturados[sold["codigo"][prd]]["Valor Faturado R$"] += sold["vlrFaturado"][prd]
            ## Devolvido
            vlrsFaturados[sold["codigo"][prd]]["Valor Devolvido R$"] += sold["vlrDevolvido"][prd]
            vlrsFaturados[sold["codigo"][prd]]["Volume Devolvido"] += sold["volDevolvido"][prd]
            # vlrsFaturados[sold["codigo"][prd]]["Volume Devolvido"][1] = sold["medida"][prd]
            ## Bonificacao
            utilização = sold['tipoNota'][prd]
            if (
                utilização == "Degustação/Consumo" 
                or utilização == "Bonificação Casada"
                or utilização == "Bonificaç não casada"
                or utilização == "Amostra Grátis"
                or utilização == "Brindes"
                ):
                vlrsFaturados[sold["codigo"][prd]]["Volume Bonificado"] += sold["qtdAtingidaKG"][prd]
                # vlrsFaturados[sold["codigo"][prd]]["Volume Bonificado"][1] = sold["medida"][prd]
                vlrsFaturados[sold["codigo"][prd]]["Valor Bonificado R$"] += sold["vlrFaturado"][prd]
            ## Desconto Financeiro
            rede = sold["grupoCliente"][prd]
            if (utilização == "Compra/Venda Comerc" or utilização == "Compra/Venda Industr"):
                for desc in range(len(descontos["grupo"])):
                    if (rede == descontos["grupo"][desc]):
                        vlrsFaturados[sold["codigo"][prd]]["Desconto Financeiro R$"] += (sold["vlrFaturado"][prd] * descontos["desc"][desc] / 100)
                        
        ## Devolucoes
        if (sold["doc"][prd] == "Dev.Nota Fiscal de Saída"):
            if (sold["responsabilidade"][prd] == "COMERCIAL"):
                if (sold["motivoDevolucao"][prd] != "DEVOLUCAO DE TROCA") and (sold["motivoDevolucao"][prd] != "VENCIDO"):
                    devolucoesComerciais += sold["vlrDevolvido"][prd]
        
        if (sold["produto"][prd] == "ACORDO COMERCIAL"):
            descAcordosComerciais += sold["vlrDevolvido"][prd]
    
    # print(json.dumps(vlrsFaturados, indent=1, cls=DecimalEncoder))

#--------------- DECLARACAO DE COLUNAS ADICIONAIS E DE RESULTADO DE CALCULOS NA TABELA FINAL --------------------------
    output = pd.DataFrame(vlrsFaturados).transpose()
    output["Desconto Acordo Comercial R$"] = Decimal(0.0)
    output["Deflator R$"] = 0.0
    output["Desconto Acordo Comercial R$"]["PA000008"] = descAcordosComerciais
    output["Faturado Líquido R$"] = 0.0
    output["% Real"] = 0.0
    output["Comissão R$"] = 0.0

    ## Aplicacao de calculo de faturamento liquido e porcentagem de comissao
    for i in range(len(output["Faturado Líquido R$"])):
        output["Faturado Líquido R$"][i] = round(output["Valor Faturado R$"][i] - (output["Valor Devolvido R$"][i] + output["Valor Bonificado R$"][i] + output["Desconto Financeiro R$"][i] + output["Desconto Acordo Comercial R$"][i]), 3)
        if output["Meta"][i] != 0 :
            output["% Real"][i] = output["Volume"][i] / output["Meta"][i]
        else: 
            output["% Real"][i] = 0

    for i in range(len(output["Comissão R$"])):
        if output["Tipo Comissao"][i] == "GRUPO":
            porcentGrupo = 100 * (output["Volume"].loc[output["Grupo Produto"] == output["Grupo Produto"][i]].sum() / output["Meta"].loc[output["Grupo Produto"] == output["Grupo Produto"][i]].sum())
            
            # print(f'{rotaDesejada} - {output["Produto"].loc[output["Grupo Produto"] == output["Grupo Produto"][i]]}: {output["Volume"].loc[output["Grupo Produto"] == output["Grupo Produto"][i]].sum() / output["Meta"].loc[output["Grupo Produto"] == output["Grupo Produto"][i]].sum()}')
            print(faixasComissoes["Comissão 3%"].values[0], '\n\n')
            
            if porcentGrupo == 0:
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 3%"].values[0]
            elif porcentGrupo < faixasComissoes["Faixa 1%"].values[0]: 
                print(f'2 - {porcentGrupo} < {faixasComissoes["Faixa 1%"].values[0]}')
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 1%"].values[0]
            elif porcentGrupo < faixasComissoes["Faixa 2%"].values[0]: 
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 2%"].values[0]
            else:
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 3%"].values[0]   
        else:
            if output["% Real"][i] == 0:
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 3%"].values[0]
            elif output["% Real"][i] < faixasComissoes["Faixa 1%"].values[0]: 
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 1%"].values[0]
            elif output["% Real"][i] < faixasComissoes["Faixa 2%"].values[0]: 
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 2%"].values[0]
            else:
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 3%"].values[0]

    if (output["Faturado Líquido R$"].sum() != 0 and devolucoesComerciais / output["Faturado Líquido R$"].sum()) > (1.5 / 100):
        output["Deflator R$"]["PA000008"] = 600        

    output = output.reset_index().rename(columns={'index':'Código Produto'})
    output= output.sort_values("Grupo Produto")
    # print(output)
    return output

#--------------- INSERCAO DE TABELA COMPLETA NO SAP VIA INSER QUERY --------------------------
query = SAPconexao.cursor()
ultimoCodigo = query.execute(
    """
    SELECT MAX((TO_INTEGER("Code"))) FROM SBO_SITIO_TESTE."@COM_CALC_COMISSAO"
    """
)

for row in query:
        if row[0] != None:
            ultimoCodigo = row[0]
        print(ultimoCodigo)

for i in range(len(rotas)):
    print(rotas[i])
    tabelaComissao = calculoComissao(rotaDesejada=rotas[i],timestamp=timestampAtual)
    # Neste passo as informações de cada linha do dataframe é separada para o INSERT
    # Os cabecalhos das linhas iniciam de 1, e nao estao sendo referenciados pelo nome pois nao sao strings com caracteres simples
    for row in tabelaComissao.itertuples():
        rowCode = f"{row[5].year}-{row[5].month:02}-{row[6]}-{row[1]}"
        # print(rowCode)
        query.execute(
            rf"""
            SELECT
            * 
            FROM SBO_SITIO_TESTE."@COM_CALC_COMISSAO" ccc 
            WHERE ccc."Name" = '{rowCode}'
        """ 
        )

        verificadorLinhaID = 0
        for checkRow in query:
            verificadorLinhaID += 1
        if verificadorLinhaID == 0:
            ultimoCodigo += 1
            query.execute(
                rf"""
                INSERT INTO
                SBO_SITIO_TESTE."@COM_CALC_COMISSAO"
                VALUES (
                    {ultimoCodigo},
                    '{rowCode}',
                    '{row[1]}',
                    '{row[2]}',
                    '{row[3]}',
                    '{row[4]}',
                    '{row[6]}',
                    '{row[7]}',
                    {row[8]},
                    {row[9]},
                    {row[10]},
                    {row[11]},
                    {row[12]},
                    {row[13]},
                    {row[14]},
                    {row[15]},
                    {row[16]},
                    {row[17]},
                    {row[18]},
                    {row[19]},
                    {row[20]},
                    '{row[5]}'
                    )
                """
            )
        else:
            # print(row)
            query.execute(
                f"""
                UPDATE SBO_SITIO_TESTE."@COM_CALC_COMISSAO" ccc
                SET 
                    --ccc."Code" = {ultimoCodigo},
                    --ccc."Name" = '{rowCode}',
                    ccc."U_Cod_Produto" = '{row[1]}',
                    ccc."U_Desc_Produto" = '{row[2]}',
                    ccc."U_Grupo_Produto" = '{row[3]}',
                    ccc."U_Tipo_Comissao" = '{row[4]}',
                    ccc."U_Rota" = '{row[6]}',
                    ccc."U_Vendedor" = '{row[7]}',
                    ccc."U_Meta" = {Decimal(row[8])},
                    ccc."U_Volume" = {Decimal(row[9])},
                    ccc."U_Volume_Devolvido" = {Decimal(row[10])},
                    ccc."U_Volume_Bonificado" = {Decimal(row[11])},
                    ccc."U_Valor_Faturado" = {Decimal(row[12])},
                    ccc."U_Valor_Devolvido" = {Decimal(row[13])},
                    ccc."U_Valor_Bonificado" = {Decimal(row[14])},
                    ccc."U_Desconto_Financeiro" = {Decimal(row[15])},
                    ccc."U_Desconto_Acordo" = {Decimal(row[16])},
                    ccc."U_Deflator" = {Decimal(row[17])},
                    ccc."U_Faturado_Liquido" = {Decimal(row[18])},
                    ccc."U_Atingido_Porcentagem" = {Decimal(row[19])},
                    ccc."U_Valor_Comissao" = {Decimal(row[20])},
                    ccc."U_Data" = '{row[5]}'
                WHERE
                    ccc."Name" = '{rowCode}'
                """
            )

query.close()