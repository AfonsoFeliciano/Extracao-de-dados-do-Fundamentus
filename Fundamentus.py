# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Realizando a instalação da biblioteca

# COMMAND ----------

pip install fundamentus

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Importando as bibliotecas necessárias na utilização deste código

# COMMAND ----------

import fundamentus
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit, regexp_replace, col, when
from pyspark.sql import DataFrame
from functools import reduce
import  os
from pyspark import SparkConf
from pyspark.sql.types import *
spark_config = SparkConf().getAll()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Definindo uma lista de ações para realização das buscas

# COMMAND ----------

lista_acoes = ['LWSA3','ELMD3','RAIL3','SCAR3','BRPR3','GEPA3','GEPA4','RCSL3','MAPT4','AALR3','ORVR3','DASA3','HETA4','SEQL3','KRSA3','RRRP3','ONCO3','RNEW3','RCSL4','RNEW11','CASH3','RNEW4','CEED4','HBSA3','TELB3','SMFT3','BKBR3','OPCT3','IFCM3','CEED3','COGN3','TCNO3','VIIA3','BRFS3','MSPA4','AHEB6','BIOM3','ANIM3','WEST3','TCNO4','MEAL3','CLSA3','NORD3','CRDE3','TELB4','DOTZ3','AMAR3','CEDO3','SHOW3','IRBR3','TXRX3','NINJ3','MBLY3','BAHI3','CVCB3','ADHM3','BLUT3','ALPK3','CEDO4','CTNM3','ENJU3','FRIO3','GOLL4','BLUT4','VIVR3','TEND3','BBRK3','PMAM3','AVLL3','TXRX4','CTNM4','TCSA3','OIBR4','BDLL3','IGBR3','HOOT4','PLAS3','SGPS3','BDLL4','OIBR3','JFEN3','RPMG3','ATMP3','MTIG4','SNSY5','RSID3','LLIS3','MNDL3','TEKA4','APER3','GPAR3','MATD3','MEGA3','PDTC3','INEP3','INEP4','SLED4','MWET4','OSXB3','MWET3','SLED3','MGEL4','PDGR3','GPIV33','SYNE3','GSHP3','FHER3','BRKM6','BRAP3','BAZA3','USIM3','USIM5','BRAP4','EUCA4','USIM6','HBTS5','BRKM3','GOAU3','BRKM5','FRTA3','CTKA4','MNPR3','GGBR3','PCAR3','PETR4','GOAU4','MRFG3','EUCA3','EPAR3','PETR3','CEAB3','ETER3','CGRA4','TASA4','CGRA3','TASA3','HAGA4','GGBR4','JBSS3','SUZB3','TKNO4','CLSC3','PTNT4','CSNA3','CPLE3','CLSC4','BNBR3','MYPK3','VALE3','DEXP4','EALT4','HBRE3','DMMO3','NUTR3','CPLE11','CPLE6','DEXP3','RAPT3','BGIP4','UNIP3','ALLD3','ENAT3','CTSA4','BBAS3','JHSF3','HBOR3','POMO3','UNIP6','RAPT4','BRSR6','SAPR3','PLPL3','CEEB5','DXCO3','UNIP5','SAPR11','SAPR4','ROMI3','PEAB3','AGRO3','CEEB3','AZUL4','CMIG4','TRPL4','BOBR4','NEOE3','MLAS3','ENGI4','CRPG6','EALT3','BRSR3','CRPG5','PTBL3','AFLT3','RANI3','POMO4','ENBR3','CSRN6','CRPG3','ABCB4','AZEV4','MTSA4','MDNE3','LAVV3','CMIN3','LOGG3','BMEB4','LEVE3','ITSA4','BEES3','ITSA3','BEES4','EKTR4','MRVE3','CRIV3','ATOM3','PEAB4','FESA3','CYRE3','CURY3','FESA4','EKTR3','ESPA3','BGIP3','MOVI3','ALUP4','ALUP11','UCAS3','ENGI11','TAEE11','TAEE4','CSRN5','ALUP3','TAEE3','CAMB3','DOHL4','CEBR5','CRIV4','CSRN3','CEBR6','KLBN4','CSAN3','HAGA3','EVEN3','KLBN11','TRPL3','JSLG3','EQTL3','SOND5','GFSA3','POSI3','CMIG3','SANB3','EQMA3B','PFRM3','TRIS3','CBAV3','CAML3','PATI4','TECN3','BMEB3','SOND6','RSUL4','COCE5','CEBR3','AZEV3','SANB11','PTNT3','REDE3','BBDC3','CCRO3','ITUB3','EZTC3','KEPL3','CGAS3','BRIV3','BMGB4','AURA33','JOPA3','CGAS5','KLBN3','SOND3','SANB4','SLCE3','GETT4','GETT3','CEPE5','CPFE3','SHUL4','LUPA3','WHRL4','CEPE6','VULC3','BPAC5','BRIV4','WHRL3','BALM4','BRSR5','WIZS3','MELK3','JALL3','CARD3','ITUB4','CTSA3','PSSA3','LVTC3','TPIS3','BBDC4','GUAR3','PATI3','VBBR3','LIGT3','EQPA3','VVEO3','ELET6','ELET3','TGMA3','LPSB3','PRIO3','DIRR3','OFSA3','CSMG3','WLMM3','QUAL3','EMAE4','ENGI3','MODL4','TTEN3','BTTL3','BALM3','SIMH3','TIMS3','PORT3','SBFG3','MOAR3','BSLI4','CXSE3','SBSP3','BRBI11','BMKS3','RECV3','CIEL3','WLMM4','MODL11','CPLE5','TUPY3','GMAT3','EQPA5','PARD3','BPAC11','SOJA3','SMTO3','BBSE3','LCAM3','PRNR3','BPAN4','UGPA3','MTRE3','AESB3','MILS3','ASAI3','MODL3','GRND3','ODPV3','VIVT3','BLAU3','MERC4','CRFB3','MRSA6B','MRSA6B','FLRY3','VITT3','BMOB3','BMIN4','ENMT4','ENMT3','BSLI3','FRAS3','FIQE3','BOAS3','ENEV3','ECOR3','ABEV3','MDIA3','VIVA3','B3SA3','ALSO3','TFCO4','STBP3','BEEF3','VLID3','PGMN3','HYPE3','CSED3','AGXY3','RENT3','RPAD6','JPSA3','LOGN3','RAIZ4','BPAC3','EGIE3','BMIN3','PNVL3','ARZZ3','ALPA3','GGPS3','BRGE3','AMBP3','INTB3','IGTI3','ALPA4','YDUQ3','IGTI11','MULT3','LUXM4','SOMA3','DESK3','BRGE6','LREN3','WEGE3','RPAD3','AMER3','BRGE8','LIPR3','VAMO3','BRGE5','EEEL4','EEEL3','LJQQ3','PINE4','RPAD5','BRML3','SULA4','SULA11','SULA3','MERC3','RDNI3','NGRD3','NTCO3','SQIA3','RDOR3','TOTS3','RADL3','CEGR3','BRIT3','AERI3','BAUH4','CBEE3','ARML3','DMVF3','PETZ3','MGLU3','EMBR3','SEER3','BIDI3','HAPV3','BIDI4','BIDI11','AURE3','TRAD3']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando o consumo da lista de ações e salvando o conteúdo em um dataframe
# MAGIC 
# MAGIC Por se tratar de uma lista com um número relativamente alto de ações, é normal se o processo de consumo demorar, visto que a busca é feita de maneira individual, percorrendo toda a listagem.

# COMMAND ----------

df_acoes_fundamentus = fundamentus.get_papel(lista_acoes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resetando o index do dataframe para leitura em spark

# COMMAND ----------

df_acoes_fundamentus.reset_index(drop=False, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizando o conteúdo do dataframe após resetar o seu index, que no caso era o ticker (código da empresa)

# COMMAND ----------

df_acoes_fundamentus

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definindo o schema para leitura dos dados em spark dataframe

# COMMAND ----------

schema = StructType([ StructField("index", StringType(), True)\
                       ,StructField("Tipo", StringType(), True) \
                       ,StructField("Empresa", StringType(), True) \
                       ,StructField("Setor", StringType(), True) \
                       ,StructField("Subsetor", StringType(), True) \
                       ,StructField("Cotacao", StringType(), True) \
                       ,StructField("Data_ult_cot", StringType(), True) \
                       ,StructField("Min_52_sem", StringType(), True) \
                       ,StructField("Max_52_sem", StringType(), True) \
                       ,StructField("Vol_med_2m", StringType(), True) \
                       ,StructField("Valor_de_mercado", StringType(), True) \
                       ,StructField("Valor_da_firma", StringType(), True) \
                       ,StructField("Ult_balanco_processado", StringType(), True) \
                       ,StructField("Nro_Acoes", StringType(), True) \
                       ,StructField("PL", StringType(), True) \
                       ,StructField("PVP", StringType(), True) \
                       ,StructField("PEBIT", StringType(), True) \
                       ,StructField("PSR", StringType(), True) \
                       ,StructField("PAtivos", StringType(), True) \
                       ,StructField("PCap_Giro", StringType(), True) \
                       ,StructField("PAtiv_Circ_Liq", StringType(), True) \
                       ,StructField("Div_Yield", StringType(), True) \
                       ,StructField("EV_EBITDA", StringType(), True) \
                       ,StructField("EV_EBIT", StringType(), True) \
                       ,StructField("Cres_Rec_5a", StringType(), True) \
                       ,StructField("LPA", StringType(), True) \
                       ,StructField("VPA", StringType(), True) \
                       ,StructField("Marg_Bruta", StringType(), True) \
                       ,StructField("Marg_EBIT", StringType(), True) \
                       ,StructField("Marg_Liquida", StringType(), True) \
                       ,StructField("EBIT_Ativo", StringType(), True) \
                       ,StructField("ROIC", StringType(), True) \
                       ,StructField("ROE", StringType(), True) \
                       ,StructField("Liquidez_Corr", StringType(), True) \
                       ,StructField("Div_Br_Patrim", StringType(), True) \
                       ,StructField("Giro_Ativos", StringType(), True) \
                       ,StructField("Ativo", StringType(), True) \
                       ,StructField("Disponibilidades", StringType(), True) \
                       ,StructField("Ativo_Circulante", StringType(), True) \
                       ,StructField("Div_Bruta", StringType(), True) \
                       ,StructField("Div_Liquida", StringType(), True) \
                       ,StructField("Patrim_Liq", StringType(), True) \
                       ,StructField("Receita_Liquida_12m", StringType(), True) \
                       ,StructField("EBIT_12m", StringType(), True) \
                       ,StructField("Lucro_Liquido_12m", StringType(), True) \
                       ,StructField("Receita_Liquida_3m", StringType(), True) \
                       ,StructField("EBIT_3m", StringType(), True) \
                       ,StructField("Lucro_Liquido_3m", StringType(), True) \
                       ,StructField("Cart_de_Credito", StringType(), True) \
                       ,StructField("Depositos", StringType(), True) \
                       ,StructField("Result_Int_Financ_12m", StringType(), True) \
                       ,StructField("Rec_Servicos_12m", StringType(), True) \
                       ,StructField("Result_Int_Financ_3m", StringType(), True) \
                       ,StructField("Rec_Servicos_3m", StringType(), True)  
                      ]
                     )



# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando leitura em spark
# MAGIC 
# MAGIC Caso ocorram erros de tipagem, ou outros, é necessário executar o comando abaixo, para ser ignorado.
# MAGIC 
# MAGIC ```
# MAGIC spark_config = SparkConf().getAll()
# MAGIC ```
# MAGIC 
# MAGIC O comando já foi executado previamente no momento da importação das bibliotecas.

# COMMAND ----------

df_acoes = spark.createDataFrame(df_acoes_fundamentus, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando transformações no dataframe final

# COMMAND ----------


df_acoes_transformed = df_acoes.select([when(col(c) == "-", None).otherwise(col(c)).alias(c) for c in df_acoes.columns]) \
            .select([when(col(c) == "NaN", None).otherwise(col(c)).alias(c) for c in df_acoes.columns]) \
            .withColumn('Cotacao', regexp_replace('Cotacao', ',', '').cast('decimal(12,2)')) \
            .withColumn('Data_ult_cot', regexp_replace('Data_ult_cot', ',', '').cast('date')) \
            .withColumn('Min_52_sem', regexp_replace('Min_52_sem', ',', '').cast('decimal(12,2)')) \
            .withColumn('Max_52_sem', regexp_replace('Max_52_sem', ',', '').cast('decimal(12,2)')) \
            .withColumn('Vol_med_2m', regexp_replace('Vol_med_2m', ',', '').cast('int')) \
            .withColumn('Valor_da_firma', regexp_replace('Valor_da_firma', ',', '').cast('int')) \
            .withColumn('Ult_balanco_processado', regexp_replace('Ult_balanco_processado', ',', '').cast('date')) \
            .withColumn('Nro_Acoes', regexp_replace('Nro_Acoes', ',', '').cast('int')) \
            .withColumn('PL', (col('PL') / 100).cast('decimal(12,2)') ) \
            .withColumn('PVP', (col('PVP') / 100).cast('decimal(12,2)') ) \
            .withColumn('PEBIT', (col('PEBIT') / 100).cast('decimal(12,2)') ) \
            .withColumn('PSR', (col('PSR') / 100).cast('decimal(12,2)') )  \
            .withColumn('PAtivos', (col('PAtivos') / 100).cast('decimal(12,2)') ) \
            .withColumn('PCap_Giro', (col('PCap_Giro') / 100).cast('decimal(12,2)') ) \
            .withColumn('PAtiv_Circ_Liq', (col('PAtiv_Circ_Liq') / 100).cast('decimal(12,2)') ) \
            .withColumn('Div_Yield', (regexp_replace('Div_Yield', '%', '') / 100).cast('decimal(12,2)') ) \
            .withColumn('EV_EBITDA', (col('EV_EBITDA') / 100).cast('decimal(12,2)') ) \
            .withColumn('EV_EBIT', (col('EV_EBIT') / 100).cast('decimal(12,2)') ) \
            .withColumn('Cres_Rec_5a', (regexp_replace('Cres_Rec_5a', '%', '') / 100).cast('decimal(12,2)') ) \
            .withColumn('LPA', (col('LPA') / 100).cast('decimal(12,2)') ) \
            .withColumn('Marg_Bruta', (regexp_replace('Marg_Bruta', '%', '') / 100).cast('decimal(12,2)') ) \
            .withColumn('Marg_EBIT', (regexp_replace('Marg_EBIT', '%', '') / 100).cast('decimal(12,2)') ) \
            .withColumn('Marg_Liquida', (regexp_replace('Marg_Liquida', '%', '') / 100).cast('decimal(12,2)') ) \
            .withColumn('EBIT_Ativo', (regexp_replace('EBIT_Ativo', '%', '') / 100).cast('decimal(12,2)') ) \
            .withColumn('ROIC', (regexp_replace('ROIC', '%', '') / 100).cast('decimal(12,2)') ) \
            .withColumn('ROE', (regexp_replace('ROE', '%', '') / 100).cast('decimal(12,2)') ) \
            .withColumn('Liquidez_Corr', (col('Liquidez_Corr') / 100).cast('decimal(12,2)') ) \
            .withColumn('Div_Br_Patrim', (col('Div_Br_Patrim') / 100).cast('decimal(12,2)') ) \
            .withColumn('Giro_Ativos', (col('Giro_Ativos') / 100).cast('decimal(12,2)') ) \
            .withColumn('Ativo', (col('Ativo') / 100).cast('int') ) \
            .withColumn('Ativo', (col('Ativo') / 100).cast('int') ) \
            .withColumn('Disponibilidades', (col('Disponibilidades') / 100).cast('int') ) \
            .withColumn('Ativo_Circulante', (col('Ativo_Circulante') / 100).cast('int') ) \
            .withColumn('Div_Bruta', (col('Div_Bruta') / 100).cast('int') ) \
            .withColumn('Div_Liquida', (col('Div_Liquida') / 100).cast('int') ) \
            .withColumn('Patrim_Liq', (col('Patrim_Liq') / 100).cast('int') ) \
            .withColumn('Receita_Liquida_12m', (col('Receita_Liquida_12m') / 100).cast('int') ) \
            .withColumn('EBIT_12m', (col('EBIT_12m') / 100).cast('int') ) \
            .withColumn('Lucro_Liquido_12m', (col('Lucro_Liquido_12m') / 100).cast('int') ) \
            .withColumn('Receita_Liquida_3m', (col('Receita_Liquida_3m') / 100).cast('int') ) \
            .withColumn('Ebit_3m', (col('Ebit_3m') / 100).cast('int') ) \
            .withColumn('Lucro_Liquido_3m', (col('Lucro_Liquido_3m') / 100).cast('int') ) \
            .withColumn('Cart_de_Credito', (col('Cart_de_Credito') / 100).cast('int') ) \
            .withColumn('Depositos', (col('Depositos') / 100).cast('int') ) \
            .withColumn('Result_Int_Financ_12m', (col('Result_Int_Financ_12m') / 100).cast('int') ) \
            .withColumn('Rec_Servicos_12m', (col('Rec_Servicos_12m') / 100).cast('int') ) \
            .withColumn('Result_Int_Financ_3m', (col('Result_Int_Financ_3m') / 100).cast('int') ) \
            .withColumn('Rec_Servicos_3m', (col('Rec_Servicos_3m') / 100).cast('int') ) \
            .withColumnRenamed("index", "Papel")

# COMMAND ----------

df_acoes_transformed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando uma view temporária para manipulação dos dados em SQL

# COMMAND ----------

df_acoes_transformed.createOrReplaceTempView("VW_ACOES")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando instruções DML, neste caso, SELECT

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM VW_ACOES
# MAGIC 
# MAGIC WHERE 1=1 
# MAGIC       AND PL > 0 AND PL <= 10
# MAGIC       AND PVP > 0 AND PVP <= 5
# MAGIC       AND ROIC > 0.15
# MAGIC       AND ROE > 0.15
# MAGIC       AND DIV_YIELD > 0.05
# MAGIC       --AND DATA_ULT_COT >= '2022-015-01'
# MAGIC       AND ULT_BALANCO_PROCESSADO >= '2022-01-01'
# MAGIC       --AND SETOR = 'Máquinas e Equipamentos'
# MAGIC       
# MAGIC       
# MAGIC     ORDER BY MARG_LIQUIDA DESC, MARG_EBIT DESC, PVP ASC
# MAGIC             
# MAGIC       
# MAGIC       
# MAGIC    
# MAGIC       
