import config
import pandas as pd
from hdbcli import dbapi as db
from decimal import Decimal
import json

# Function to print json objects with decimals
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

# --------------- MAIN QUERY VENDAS --------------------
def queryVendas(colunaNome, mes, ano, rota, conexaoBD):
    data = conexaoBD.cursor()
    colunaValores = []
    if mes > 12 or mes < 0:
        raise Exception("Informe mes valido")

    anoAnt = ano
    mesAnt = mes - 1
    if mes == 1:
        mesAnt = 12
        anoAnt = anoAnt - 1
    
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

#------------------ DATABASE CONNECTION -------------------
SAPcredenciais = config.credentials["HANA"]
SAPconexao = db.connect(
    address = SAPcredenciais["address"],
    port = SAPcredenciais["port"],
    user = SAPcredenciais["user"],
    password = SAPcredenciais["password"],
)

#------------------ ARQUIVO DE SAIDA ------------------
nomeArqSaida = "Tabela Saída"
arquivoSaida = pd.ExcelWriter(
    rf"Calculo Comissao\Calculos Gerados\{nomeArqSaida}.xlsx",
    engine="xlsxwriter",
    mode="w",
)

#------------------ VARIAVEIS INPUTAVEIS ------------------
## Mês desejado
mesDesejado = 2
mes = 2
meses = [1,2,3]
ano = 2022, 2023
## Rota desejada
rotaDesejada = "A2"
rotas = [
    "A1",
    "A2",
    "G1",
    "G2",
    "G3",
    "A3",
    "G5",
    "G8",
    "G9"
]

arqEntrada = r"Calculo Comissao\Entradas\Entrada2-23.xlsx"

def calculoComissao(mesDesejado, anoDesejado, rotaDesejada, excelEntrada):
    #------------------ DESCONTOS P/ REDE ------------------
    arquivoEntrada = pd.ExcelFile(excelEntrada)
    descontos = pd.read_excel(arquivoEntrada,"Descontos")
    descontos = {
        "grupo": descontos["Rede"].values.tolist(),
        "cliente": descontos["Cliente"].values.tolist(),
        "desc": descontos["Desc Financeiro"].values.tolist(),
    }
    # print(descontos)

    #------------------ COMISSOES P/ VENDEDOR ------------------
    arquivoEntrada = pd.ExcelFile(excelEntrada)
    comissoes = pd.read_excel(arquivoEntrada,"Faixa Comissão")
    faixasComissoes = comissoes.loc[comissoes["Rota"] == rotaDesejada]
    # print(comissoes)

    #------------------ META p/ VENDEDOR ------------------
    arquivoEntrada = pd.ExcelFile(excelEntrada)
    meta = pd.read_excel(arquivoEntrada,"Meta")
    meta = {
        "codigo": meta["Código"].values.tolist(),
        "produto": meta["Produto"].values.tolist(),
        "grupo": meta["Grupo"].values.tolist(),
        "marca": meta["Marca"].values.tolist(),
        "medida": meta["Medida"].values.tolist(),
        "vendedor": meta["Vendedor"].values.tolist(),
        "rota": meta["Rota"].values.tolist(),
        "qt": meta["Quantidade"].values.tolist(),
        "tipoComissao": meta["Tipo Comissão"].values.tolist(),
    }
    meta = pd.DataFrame(meta)
    meta = meta.loc[meta["rota"] == rotaDesejada]
    # print(meta)

    ## DEPOIS PUXAR PRODUTOS DO SAP
    produtos = pd.read_excel(arquivoEntrada,"Produtos")
    produtos = {
        "cod": produtos["Codigo_Item"].values.tolist(),
        "desc": produtos["Descrição do item/serviço"].values.tolist(),
        "grupo": produtos["Grupo_de_Produto"].values.tolist(),
        "marca": produtos["Marca"].values.tolist()
    }
    # print(products)
        
    #------------------ IMPORTANT COLUMNS ------------------
    sold = {
        "rota": queryVendas("Rota", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "produto": queryVendas(r"Descrição do item/serviço",  mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "codigo": queryVendas("Nº do item", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),    
        "vendedor": queryVendas("Vendedor", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),    
        "grupo": queryVendas("Grupo de itens", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        # "marca": queryVendas("Marca", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "grupoCliente": queryVendas("Grupo cliente", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "qtdAtingidaKG": queryVendas("Em KG", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "vlrFaturado": queryVendas("Vlr.Faturado", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "volDevolvido": queryVendas("Qtd.Devolvida", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "vlrDevolvido": queryVendas("Vlr.Devolvido", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "medida": queryVendas("Código da UM", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "tipoNota": queryVendas("Utilização", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "responsabilidade": queryVendas("Responsável Devolução", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "motivoDevolucao": queryVendas("Motivo da Devolução", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "doc": queryVendas("Documento", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        "data": queryVendas("Data de lançamento", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
        # --- BONFICACAO --- "": queryVendas("", mesDesejado, anoDesejado, rotaDesejada, SAPconexao),
    }
    # print(pd.DataFrame(sold))

    # --------------- MAIN QUERY METAS NÃO SERÁ UTILIZADA A PRINCIPIO --------------------
    def queryMeta(colunaNome, mes, rota, conexaoBD):
        data = conexaoBD.cursor()
        colunaValores = []

        sqlPRD = f"""
        SELECT 
            mv."{colunaNome}"
        FROM
            META_VENDAS mv
        WHERE 
            mv."Data de lançamento" >= '{anoDesejado}-{mes - 1}-26' 
            AND mv."Data de lançamento" <= '{anoDesejado}-{mes}-25'
            AND mv."Rota" = '{rota}'
        """
        data.execute(sqlPRD)

        for row in data:
            colunaValores.append(row)
        data.close()    
        
        return colunaValores

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

    for prd in range(len(lsProdutos)):
        vlrsFaturados[lsProdutos[prd][0]] = {
            "Produto": lsGrupoProduto["nome"].loc[lsGrupoProduto["cod"] == lsProdutos[prd][0]].values.item(),
            "Grupo Produto": lsGrupoProduto["grupo"].loc[lsGrupoProduto["cod"] == lsProdutos[prd][0]].values.item(),
            "Tipo Comissao": meta["tipoComissao"].loc[lsProdutos[prd][0] == meta["codigo"]].values.item() if len(meta["tipoComissao"].loc[lsProdutos[prd][0] == meta["codigo"]].values) > 0 else "SKU",
            "Mês": mesDesejado,
            "Ano": anoDesejado,
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
                        vlrsFaturados[sold["codigo"][prd]]["Desconto Financeiro R$"] += (float(sold["vlrFaturado"][prd]) * descontos["desc"][desc])
                        
        ## Devolucoes
        if (sold["doc"][prd] == "Dev.Nota Fiscal de Saída"):
            if (sold["responsabilidade"][prd] == "COMERCIAL"):
                if (sold["motivoDevolucao"][prd] != "DEVOLUCAO DE TROCA") and (sold["motivoDevolucao"][prd] != "VENCIDO"):
                    devolucoesComerciais += sold["vlrDevolvido"][prd]
        
        
        if (sold["produto"][prd] == "ACORDO COMERCIAL"):
            descAcordosComerciais += sold["vlrDevolvido"][prd]
    
    # print(json.dumps(vlrsFaturados, indent=1, cls=DecimalEncoder))

    output = pd.DataFrame(vlrsFaturados).transpose()
    output["Desconto Acordo Comercial R$"] = 0.0
    output["Deflator R$"] = 0.0
    output["Desconto Acordo Comercial R$"]["PA000008"] = descAcordosComerciais
    output["Faturado Líquido R$"] = 0.0
    output["% Real"] = 0
    output["Comissão R$"] = 0.0
    # ## Values are coming as Decimal type from SQL table, then they're converted to float
    output["Valor Faturado R$"] = output["Valor Faturado R$"].astype(float)
    output["Valor Devolvido R$"] = output["Valor Devolvido R$"].astype(float)
    output["Valor Bonificado R$"] = output["Valor Bonificado R$"].astype(float)
    output["Desconto Financeiro R$"] = output["Desconto Financeiro R$"].astype(float)
    output["Desconto Acordo Comercial R$"] = output["Desconto Acordo Comercial R$"].astype(float)

    ## Aplicacao de calculo de faturamento liquido e porcentagem de comissao
    for i in range(len(output["Faturado Líquido R$"])):
        output["Faturado Líquido R$"][i] = round(output["Valor Faturado R$"][i] - (output["Valor Devolvido R$"][i] + output["Valor Bonificado R$"][i] + output["Desconto Financeiro R$"][i] + output["Desconto Acordo Comercial R$"][i]), 3)
        if output["Meta"][i] != 0 :
            output["% Real"][i] = float(output["Volume"][i]) / output["Meta"][i]
        else: 
        #     output["% Real"][i] = "Sem meta"
            output["% Real"][i] = 0


    for i in range(len(output["Comissão R$"])):
        if output["Tipo Comissao"][i] == "GRUPO":
            porcentGrupo = (float(output["Volume"].loc[output["Grupo Produto"] == output["Grupo Produto"][i]].sum()) / output["Meta"].loc[output["Grupo Produto"] == output["Grupo Produto"][i]].sum())
            if porcentGrupo == 0:
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 3 %"].values[0]
            elif porcentGrupo < faixasComissoes["Faixa 1 %"].values[0]: 
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 1 %"].values[0]
            elif porcentGrupo < faixasComissoes["Faixa 2 %"].values[0]: 
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 2 %"].values[0]
            else:
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 3 %"].values[0]           
        else:
            if output["% Real"][i] == 0:
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 3 %"].values[0]
            elif output["% Real"][i] < faixasComissoes["Faixa 1 %"].values[0]: 
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 1 %"].values[0]
            elif output["% Real"][i] < faixasComissoes["Faixa 2 %"].values[0]: 
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 2 %"].values[0]
            else:
                output["Comissão R$"][i] = output["Faturado Líquido R$"][i] * faixasComissoes["Comissão 3 %"].values[0]
    
    # print(devolucoesComerciais, "\n\n")
    if (float(devolucoesComerciais) / output["Faturado Líquido R$"].sum()) > (1.5 / 100):
        output["Deflator R$"]["PA000008"] = 600        

    output= output.sort_values("Grupo Produto")
    # print(output)
    return output

    # gruposSemDuplicata = lsGrupoProduto["grupo"].drop_duplicates().tolist()
    # grupos = gruposSemDuplicata

    # for grp in range(len(gruposSemDuplicata)):
    #     gruposSemDuplicata[grp] = output.loc[output["Grupo Produto"] == grupos[grp]]

    # devolucaoTotal = 0
    # volumeAtingidoTotal = 0
    # tamTabela = 0
    # for grp in range(len(gruposSemDuplicata)):
    #     gruposSemDuplicata[grp]["Meta"]["Total"] = gruposSemDuplicata[grp]["Meta"].sum()
    #     gruposSemDuplicata[grp]["Volume"]["Total"] = gruposSemDuplicata[grp]["Volume"].sum()
    #     gruposSemDuplicata[grp]["Volume Devolvido"]["Total"] = gruposSemDuplicata[grp]["Volume Devolvido"].sum()
    #     gruposSemDuplicata[grp]["Volume Bonificado"]["Total"] = gruposSemDuplicata[grp]["Volume Bonificado"].sum()
        
    #     volumeAtingidoTotal += gruposSemDuplicata[grp]["Volume"].sum()
    #     devolucaoTotal += gruposSemDuplicata[grp]["Volume Devolvido"].sum()
        
    #     gruposSemDuplicata[grp]["Valor Faturado R$"]["Total"] = gruposSemDuplicata[grp]["Valor Faturado R$"].sum()
    #     gruposSemDuplicata[grp]["Valor Devolvido R$"]["Total"] = gruposSemDuplicata[grp]["Valor Devolvido R$"].sum()
    #     gruposSemDuplicata[grp]["Valor Bonificado R$"]["Total"] = gruposSemDuplicata[grp]["Valor Bonificado R$"].sum()
    #     gruposSemDuplicata[grp]["Desconto Financeiro R$"]["Total"] = gruposSemDuplicata[grp]["Desconto Financeiro R$"].sum()
    #     gruposSemDuplicata[grp]["Desconto Acordo Comercial R$"]["Total"] = gruposSemDuplicata[grp]["Desconto Acordo Comercial R$"].sum()
    #     gruposSemDuplicata[grp]["Faturado Líquido R$"]["Total"] = gruposSemDuplicata[grp]["Faturado Líquido R$"].sum()
    #     gruposSemDuplicata[grp]["% Real"]["Total"] = gruposSemDuplicata[grp]["% Real"].sum()
    #     gruposSemDuplicata[grp]["Comissão R$"]["Total"] = gruposSemDuplicata[grp]["Comissão R$"].sum()
        
    #     if tamTabela > 0:
    #         gruposSemDuplicata[grp].to_excel(outputFile, sheet_name=f"Resumo Rota {rotaDesejada}", startrow=tamTabela, header=False)
    #     else:
    #         gruposSemDuplicata[grp].to_excel(outputFile, sheet_name=f"Resumo Rota {rotaDesejada}", startrow=tamTabela)
    #         tamTabela += 1
    #     tamTabela += len(gruposSemDuplicata[grp].index) + 1

    # if rotaDesejada[0] == 'A' or rotaDesejada[0] == 'G':
    #     percentualDeflacao = devolucaoTotal / volumeAtingidoTotal
    #     if percentualDeflacao >= (1.5 / 100):
    #         pd.DataFrame({"Deflação %": [600]}).to_excel(output, sheet_name=f"Resumo Rota {rotaDesejada}", startcol=16)

    # print(output)

                                    # linhaPlanilha = 0
                                    # for m in range(1,4):
                                    #     for i in range(len(rotas)):
                                    #         print(rotas[i])
                                    #         tabelaComissao = calculoComissao(rotaDesejada=rotas[i], mesDesejado=m, anoDesejado=2023, excelEntrada=arqEntrada)
                                    #         if linhaPlanilha == 0:
                                    #             tabelaComissao.to_excel(arquivoSaida, startrow=linhaPlanilha)
                                    #         else:
                                    #             tabelaComissao.to_excel(arquivoSaida, startrow=linhaPlanilha, header=False)
                                    #         linhaPlanilha += len(tabelaComissao)

linhaPlanilha = 0

for i in range(len(rotas)):
    print(rotas[i])
    tabelaComissao = calculoComissao(rotaDesejada=rotas[i], mesDesejado=2, anoDesejado=2023, excelEntrada=arqEntrada)
    if linhaPlanilha == 0:
        tabelaComissao.to_excel(arquivoSaida, startrow=linhaPlanilha)
    else:
        tabelaComissao.to_excel(arquivoSaida, startrow=linhaPlanilha, header=False)
    linhaPlanilha += len(tabelaComissao)
        
arquivoSaida.close()
