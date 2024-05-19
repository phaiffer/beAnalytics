import pandas as pd
import pandas_gbq as gbq
from bs4 import BeautifulSoup
from selenium import webdriver

# Utilizando a biblioteca de test do chrome 
options =  webdriver.ChromeOptions()
options.add_experimental_option("prefs",{"download.propmt_for_download": False})
driver = webdriver.Chrome(options=options)

# Passando a URL que esta com as informações que precisamos captar.
driver.get("https://steamdb.info/sales/")

# ajustando o html para UTF-8 e deixando ele melhor para a leitura e captação de dados.
html = driver.page_source.encode("utf-8")
soup = BeautifulSoup(html, "lxml")
results = []

# for para pegas as informações da table e inserir no lxml
for row in soup.find_all("tr")[1:]:
    data = row.find_all("td")
    name = data[2].find("a").text
    discount = data[3].text
    price = data[4].text
    rating = data[5].text
    release = data[6].text
    ends = data[7].text
    started = data[8].text

    results.append(
        {
            "Name": name,
            "Discount": discount,
            "Price": price,
            "Rating": rating,
            "Release": release,
            "Ends": ends,
            "Started": started,
        }
    )

# Criando o DataFrame com os resultados do que foi captado na pesquisa.
df = pd.DataFrame(results)

# project_id do Google Cloud
project_id = "processo-seletivo-beanalytics"

# table_id de destino (incluindo o conjunto de dados(dataset))
table_id = "steamdb.tb_sales"

# Exportando o DataFrame para o BigQuery
gbq.to_gbq(
    df, 
    if_exists="replace", 
    destination_table=table_id, 
    project_id=project_id
    )
