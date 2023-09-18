from bs4 import BeautifulSoup
import requests
from datetime import datetime


def extract_info(html):
    def extract_hora(div_next):
        mes,ano,hora = div_next.contents[-1].replace("\n","").replace("\t","").split(",")
        mes,dia  = mes.split(" ")
        hora = hora.strip()
        dia = dia.strip()
        ano = ano.strip()
        hora,minuto,segundo = hora.split(":")
        segundo = segundo.replace("\u202f"," ")
        segundo,ampm,_ = segundo.split(" ")
        hora = int(hora)-1
        if ampm == "PM":
            hora = hora+12
        meses = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7, "Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        mes = meses[mes]
        
        fecha = datetime(int(ano),int(mes),int(dia),int(hora),int(minuto),int(segundo))
        return fecha

    soup = BeautifulSoup(html, 'html.parser')

    divs = soup.find_all('div', {'class': 'outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp'})

    info_list = []

    
    for div in divs:
        info_dict = {}
        title = div.find('p', {'class': 'mdl-typography--title'}).text.strip()
        info_dict['Lugar'] = title
        div= div.find("div")
        div_next= div.find_all("div")
        #print(div_next[1])
        links = div_next[1].find_all('a')
        datos = []
        for link in links:
            url = link.get('href')
            id = url.split('/')[-1]
            id = id.replace('watch?v=','')
            datos.append([link.text.strip(),id])

        info_dict["Video"] = datos[0][0]
        info_dict["ID_Video"] = datos[0][1]
        info_dict["Canal"] = datos[1][0]
        info_dict["ID_Canal"] = datos[1][1]
        
        info_dict['Fecha'] = extract_hora(div_next[1])



        content_cells = div.find_all('div', {'class': 'content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1'})
        for i, content_cell in enumerate(content_cells):
            info_dict[f'Content Cell {i+1}'] = content_cell.text.strip()

        

        info_list.append(info_dict)

    return info_list

if __name__ == "__main__":
    with open('S:\Github\infovis\datos_personales\\fff.html', 'r',encoding="utf-8") as f:
        html = f.read()
    data = extract_info(html)
    print(data)