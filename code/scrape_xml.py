#!/usr/bin/env python
# coding: utf-8

# In[1]:


# imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import shutil
import warnings
warnings.filterwarnings('ignore')


# In[2]:


# scraping from the main website
url = "https://www.agcom.it/dati-elementari-di-monitoraggio-televisivo"
pages = []
pages.append(url)
soup = BeautifulSoup(requests.get(url).content, "html.parser")
li = soup.find("li", {"class": "next"}) 
while (li is not None):
    url = li.find("a")['href']
    pages.append(url)
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    li = soup.find("li", {"class": "next"})  


# In[3]:


# data from each page
lefturl = "https://www.agcom.it/visualizza-documento?p_p_id=visualizzadocumento_WAR_visualizzadocumentoportlet&p_p_lifecycle=1&p_p_state=normal&p_p_mode=view&p_p_col_id=single-column&p_p_col_count=1&_visualizzadocumento_WAR_visualizzadocumentoportlet_javax.portlet.action=convertUrl&_visualizzadocumento_WAR_visualizzadocumentoportlet_uuid="
idtoscrape = []
for url in pages:
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    anchors = soup.find("div", {"id": "risultati"}).find_all("a", href=True)
    for a in anchors:
        idtoscrape.append(a['href'].replace("/visualizza-documento/",""))


# In[4]:


# download of each file 
files = []
for id in idtoscrape:
    url = lefturl + id
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    files.append(soup.find_all("a", {"class": "nopdf"})[0]['href'])


# In[5]:


# dataframe creation
data_toclean = pd.read_xml(files[0],encoding='utf-8', parser='lxml')
data_toclean = data_toclean.iloc[2:, :]
del data_toclean['TESTO']


# In[6]:


for file in files[1:]:
    df = pd.read_xml(file,encoding='utf-8', parser='lxml')
    df = df.iloc[2:, :]
    del df['TESTO']
    #data = data.append(df)
    data_toclean = pd.concat([data_toclean, df])


# In[7]:


# save original files
k = 0
for file in files:
    response = requests.get(file, stream=True)
    filename = str(k) + "_"  + file.split("+")[2].split("/")[0].replace("-", "_")+".xml.gz"
    with open(".." + os.sep + "data" + os.sep + "raw" + os.sep + filename, 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)
    k += 1


# In[8]:


# dataframe cleaning
data = data_toclean
data = data.dropna()


# In[9]:


# words to clean (?)
replace_words = [];
replace_words.append(("Pubblicita", "Pubblicità"))
replace_words.append(("Pubblicit�", "Pubblicità"))
replace_words.append(("Societa", "Società"))
replace_words.append(("Societ�", "Società"))
replace_words.append(("LAlternativa c�", "L'Alternativa c'è!"))
replace_words.append(("L�Alternativa c��", "L'Alternativa c'è!"))
replace_words.append(("l�Italia", "l'Italia"))
replace_words.append(("GAL Grandi autonomie e libert�",
                     "GAL Grandi autonomie e libertà"))
replace_words.append(("Azione - +Europa � Radicali Italiani",
                     "Azione - +Europa - Radicali Italiani"))
replace_words.append(("Popolari per l?Italia","Popolari per l'Italia"))
replace_words.append(("Libert�", "Libertà"))
replace_words.append(("Verit�", "Verità"))
replace_words.append(("Costume e societ�", "Costume e società"))
replace_words.append(("Costume e societa", "Costume e società"))
replace_words.append(("l�Europa", "l'Europa"))
replace_words.append(("/+ Europa ?Centro Democratico",
                     "+Europa - Centro Democratico"))
replace_words.append((" - Centro Democratico","Centro Democratico"))
replace_words.append(("Centro Democratico � Italiani in Europa",
                     "Centro Democratico - Italiani in Europa"))
replace_words.append(("/+ Europa", "+Europa"))
replace_words.append(("/+ Europa ?Centro Democratico",
                     "+Europa - Centro Democratico"))
replace_words.append(("Autonomia per l?Europa","Autonomia per l'Europa"))
replace_words.append(("Noi con l'Italia�USEI�Rinascimento-AdC",
                     "Noi con l'Italia - USEI - Rinascimento - Alleanza di Centro"))
replace_words.append(("Noi con l?Italia-USEI", "Noi con l'Italia-USEI"))
replace_words.append(("CasaPound Italia ? Destre Unite","CasaPound Italia -Destre Destre Unite"))
replace_words.append(("Autonomia per l?Europa",
                     "Autonomia per l'Europa"))
replace_words.append(("Popolari per l?Italia","Popolari per l'Italia"))
replace_words.append(("Politica e attivit� istituzionali",
                     "Politica e attività istituzionali"))
replace_words.append(("Politica e attivita istituzionali",
                     "Politica e attività istituzionali"))
replace_words.append((
    "Noi con l'Italia�USEI�Rinascimento-AdC", 
    "Noi con l'Italia - USEI - Rinascimento - Alleanza di Centro"))
replace_words.append(("Azione - +Europa � Radicali Italiani",
                     "Azione - +Europa - Radicali Italiani"))
replace_words.append((
    "Noi con l?Italia-USEI-Cambiamo!- Alleanza di Centro", "Noi con l'Italia - USEI - Cambiamo! - Alleanza di Centro")
)
replace_words.append(("�Centro Democratico", "Centro Democratico"))
replace_words.append(("Facciamo Eco � Federazione dei Verdi",
                     "Facciamo Eco - Federazione dei Verdi"))
replace_words.append(("NON � L'ARENA \(TESTATA\)", "NON È L'ARENA \(TESTATA\)"))
replace_words.append(("NON � L'ARENA SPECIALE \(TESTATA\)", "NON È L'ARENA SPECIALE \(TESTATA\)"))
replace_words.append(("NON � L'ARENA \(RETE\)","NON È l'ARENA \(RETE\)"))
replace_words.append(("NON E' L'ARENA PRIMA PAGINA \(TESTATA\)","NON É L'ARENA PRIMA PAGINA \(TESTATA\)"))
replace_words.append(("NON � L'ARENA - IL MEGLIO DI \(TESTATA\)","NON È L'ARENA - IL MEGLIO DI \(TESTATA\)"))
replace_words.append(("NON E' L'ARENA - IN PIAZZA A MEZZOJUSO(TESTATA)","NON È L'ARENA - IN PIAZZA A MZZOJUSO \(TESTATA\)"))
replace_words.append(("TG1 - LE CITT� D'EUROPA PER LA PACE",
                     "TG1 - LE CITTÀ D'EUROPA PER LA PACE"))
replace_words.append(("TG1 E RAI QUIRINALE - CELEBRAZIONE DEL 77� ANNIVERSARIO DELLA LIBERAZIONE",
                     "TG1 E RAI QUIRINALE - CELEBRAZIONE DEL 77° ANNIVERSARIO DELLA LIBERAZIONE"))
replace_words.append(("ANTEPRIMA AGOR� ESTATE \(TESTATA\)", "ANTEPRIMA AGORÀ ESTATE \(TESTATA\)"))    
replace_words.append(("AGOR� ESTATE \(TESTATA\)",
                     "AGORÀ ESTATE \(TESTATA\)"))
replace_words.append(("ANTEPRIMA AGORA' \(TESTATA\)",
                     "ANTEPRIMA AGORÀ ESTATE \(TESTATA\)"))
replace_words.append(("TGR - 68� RADUNO NAZIONALE BERSAGLIERI",
                     "TGR - 68° RADUNO NAZIONALE BERSAGLIERI"))
replace_words.append(("LIVE NON � LA D'URSO",
                      "LIVE NON È LA D'URSO"))
replace_words.append(("PRESENTAZIONE 104� EDIZIONE GIRO D'ITALIA",
                      "PRESENTAZIONE 104° EDIZIONE GIRO D'ITALIA"))
replace_words.append(("SANREMO 2021 - 71� FESTIVAL DELLA CANZONE ITALIANA",
                     "SANREMO 2021 - 71° FESTIVAL DELLA CANZONE ITALIANA"))
replace_words.append(("SANREMO 2020 - 70� FESTIVAL DELLA CANZONE ITALIANA",
                     "SANREMO 2020 - 70° FESTIVAL DELLA CANZONE ITALIANA"))
replace_words.append(("CAFF� EUROPA", "CAFFÈ EUROPA"))
replace_words.append(("SPECIALE CHE GIORNO �","SPECIALE CHE GIORNO È"))
replace_words.append(("TAGAD� LE STORIE /(TESTATA/)", "TAGADÀ LE STORIE /(TESTATA/)"))
replace_words.append(("TAGAD� LE STORIE (TESTATA)", "TAGADÀ LE STORIE (TESTATA)"))
replace_words.append(
    ("TG1 DIRETTA - 50� ANNIVERSARIO DELLA STRAGE DI PIAZZA FONTANA", "TG1 DIRETTA - 50° ANNIVERSARIO DELLA STRAGE DI PIAZZA FONTANA"))
replace_words.append(
    ("PRESENTAZIONE 103� EDIZIONE GIRO D'ITALIA", "PRESENTAZIONE 103° EDIZIONE GIRO D'ITALIA"))
replace_words.append(("AGOR� ESTATE SPECIALE", "AGORÀ ESTATE SPECIALE"))
replace_words.append(("ANTEPRIMA AGOR� ESTATE SPECIALE",
                     "ANTEPRIMA AGORÀ ESTATE SPECIALE"))
replace_words.append(
    ("TIKI TAKA IL CALCIO � IL NOSTRO GIOCO", "TIKI TAKA IL CALCIO È IL NOSTRO GIOCO"))
replace_words.append(
    ("TAGADA' PRIMA PAGINA (TESTATA)", "TAGADÀ PRIMA PAGINA (TESTATA)"))
replace_words.append(("TG2 - 64� REGATA DELLE ANTICHE REPUBBLICHE MARINARE", "TG2 - 64° REGATA DELLE ANTICHE REPUBBLICHE MARINARE"))
replace_words.append(("TGR - 66� RADUNO NAZIONALE BERSAGLIERI", "TGR - 66° RADUNO NAZIONALE BERSAGLIERI"))

replace_words.append(("SKY TG24 SPECIALE - CITT� AL VOTO",
                     "SKY TG24 SPECIALE - CITTÀ AL VOTO"))
replace_words.append(("SKY TG24 SPECIALE - CITT� AL VOTO \(I\)",
                     "SKY TG24 SPECIALE - CITTÀ AL VOTO \(I\)"))
replace_words.append(
    ("TG1 E RAI QUIRINALE - CELEBRAZIONE DEL 73� ANNIVERSARIO DELLA LIBERAZIONE", "TG1 E RAI QUIRINALE - CELEBRAZIONE DEL 73° ANNIVERSARIO DELLA LIBERAZIONE"))
replace_words.append(
    ("TGR - 24� RADUNO: ASSOCIAZIONE NAZIONALE CARABINIERI", "TGR - 24° RADUNO: ASSOCIAZIONE NAZIONALE CARABINIERI"))
replace_words.append(
    ("TGR - 91� ADUNATA NAZIONALE ALPINI", "TGR - 91° ADUNATA NAZIONALE ALPINI"))
replace_words.append(
    ("LA CADUTA � STORIA DI UNA MORTE, DI UNA BANCA, DI UNA CITT�", "LA CADUTA - STORIA DI UNA MORTE, DI UNA BANCA, DI UNA CITTÀ"))
replace_words.append(("Jos�", "José"))
replace_words.append(("Mul�", "Mulé"))
replace_words.append(("Diventer�", "Diventerà"))
replace_words.append(("Union Vald�taine", "Union Valdôtaine"))
replace_words.append(("D'Inc�", "D'Incà"))
replace_words.append(("Santanch�", "Santanchè"))
replace_words.append(("Dess�", "Dessì"))
replace_words.append(("Bonaf�", "Bonafè"))
replace_words.append(("Miccich�", "Miccichè"))
replace_words.append(("Cirinn�", "Cirinnà"))
replace_words.append(("Patan�", "Patanè"))
replace_words.append(("Niccol�", "Niccolò"))
replace_words.append(("Nicol�", "Nicolò"))
replace_words.append(("Samon�", "Samonà"))
replace_words.append(("Cant�", "Cantù"))
replace_words.append(("Curr�", "Currò"))
replace_words.append(("Carl�","Carlà"))
replace_words.append(("Zuccal�", "Zuccalà"))
replace_words.append(("Mont�", "Montà"))
replace_words.append(("Bertol�", "Bertolè"))
replace_words.append(("Movimento Verde � Popolare","Movimento Verde è Popolare"))
replace_words.append(("Spirl�", "Spire qui viene applicata la pulizialì"))
replace_words.append(("Tot�", "Totò"))
replace_words.append(("Gioffr�", "Gioffrè"))
replace_words.append(("Gran�", "Granà"))
replace_words.append(("Ciag�", "Ciagà"))
replace_words.append(("And�", "Andò"))
replace_words.append(("Salm�", "Salmè"))
replace_words.append(("Cass�", "Cassì"))
replace_words.append(("Comitato io Dico S�", "Comitato io Dico Sì"))
replace_words.append(("Movimento 3V \(Vaccini Vogliamo Verit�\)", "Movimento 3V \(Vaccini Vogliamo Verità\)"))
replace_words.append(("Lealt� Azione", "Lealtà Azione"))
replace_words.append(("LAlternativa c�- Lista del Popolo per la Costituzione", "L'alternativa c'è - Lista del Popolo per la Costituzione"))
replace_words.append(("Carl� Campa", "Carlà Campa"))
replace_words.append(("Falcomat�", "Falcomatà"))
replace_words.append(("Urz�", "Urzì"))
replace_words.append(("Mazz�", "Mazzà"))
replace_words.append(("C�cile", "Cécile"))
replace_words.append(("Giosu�", "Giosué"))
replace_words.append(("Pal�", "Palù"))
replace_words.append(("3V Verit� e libert�", "3V Verità e libertà"))
replace_words.append(("German�", "Germanà"))
replace_words.append(("Mamm�", "Mammì"))
replace_words.append(("Petten�", "Pettenà"))
replace_words.append(("LAlternativa c�!", "L'alternativa c´è!"))
replace_words.append(("Pratic�", "Praticò"))
replace_words.append(("Pellican�", "Pellicanò"))
replace_words.append(("Muscar�", "Muscarà"))
replace_words.append(("Carr�", "Carrà"))
replace_words.append(("L�Alternativa c��!", "L'alternativa c'è!"))
replace_words.append(("Cannistr�", "Cannistrà"))
replace_words.append(("Aric�", "Aricò"))
replace_words.append(("Facciamo Eco � Federazione dei Verdi", "Facciamo Eco - Federazione dei Verdi"))
replace_words.append(("Orr�", "Orrù"))
replace_words.append(("Azione - +Europa ? Radicali Italiani", "Azione - +Europa - Radicali Italiani"))
replace_words.append(("Azione - +Europa � Radicali Italiani",
                     "Azione - +Europa - Radicali Italiani"))
replace_words.append(("Id�", "Idà"))
replace_words.append(("Giamp�", "Giampà"))
replace_words.append(("Ver�", "Verì"))
replace_words.append(("Benvegn�", "Benvegnù"))
replace_words.append(("Il sì delle Libertà", "Il s� delle Libert�"))
replace_words.append(("Vall�e d'Aoste Unie", "Vallée d'Aoste Unie"))
replace_words.append(("Marcian�", "Marcianò"))
replace_words.append(("S� Toscana a Sinistra", "Sì Toscana a Sinistra"))
replace_words.append(("Comitato Democratici per il S�",
                     "Comitato Democratici per il Sì"))
replace_words.append(("Ingrill�", "Ingrillì"))
replace_words.append(("Serran�", "Serranò"))
replace_words.append(("GAL Grandi autonomie e liber�",
                     "GAL Grandi autonomie e libertà"))
replace_words.append(("Soggetto collettivo", "Soggetto Collettivo"))
replace_words.append(("Soggettivo Collettivo", "Soggetto Collettivo"))
replace_words.append(("Pi� Europa-Centro Democratico",
                     "+Europa-Centro Democratico"))
replace_words.append(("Schir�", "Schirò"))
replace_words.append(("Gull�", "Gullì"))
replace_words.append(("Movimento Nazionale per la Sovranit�",
                     "Movimento Nazionale per la Sovranità"))
replace_words.append(("Giuffr�", "Giuffrè"))
replace_words.append(("Scott�", "Scottà"))
replace_words.append(("Chiamparino per il Piemonte del s�",
                     "Chiamparino per il Piemonte del sì"))
replace_words.append(("Pavor�", "Pavorè"))
replace_words.append(("Span�", "Spanò"))
replace_words.append(("Ven�", "Venè"))
replace_words.append(("Cu�", "Cuè"))
replace_words.append(("Pi� Europa", "+Europa"))
replace_words.append(("CasaPound Italia ? Destre Unite",
                     "CasaPound Italia-Destre Unite"))
replace_words.append(("/+ Europa","+Europa"))
replace_words.append(("Popolari per l?Italia", "Popolari per l'Italia"))
replace_words.append(("Al� Listi", "Alì Listi"))
replace_words.append(("L'Italia � popolare", "L'Italia è popolare"))
replace_words.append(("Democrazia � Libert� - La Margherita",
                     "Democrazia è Libertà - La Margherita"))
replace_words.append(("R�sch", "Rösch"))
replace_words.append(("Ragione e Libert�", "Ragione e Libertà"))
replace_words.append(
    ("Tradition et progr�s-Vall�e d'Aoste", "Tradition et Progrès - Vallée d'Aoste"))
replace_words.append(("Ballar�", "Ballarè"))
replace_words.append(("Chitt�", "Chittò"))
replace_words.append(("Ballar�", "Ballarè"))
replace_words.append(("Alver�", "Alverà"))
replace_words.append(("Scutell�", "Scutellà"))
replace_words.append(("Desir�", "Desirè"))
replace_words.append(("Deh�", "Dehò"))
replace_words.append(("Dacc�", "Daccò"))
replace_words.append(("D�Alberto","D'Alberto"))
replace_words.append(("Niceta Pot�", "Niceta Potì"))
replace_words.append(("Wagu�", "Wagué"))
replace_words.append(("Cal� Lesina", "Calà Lesina"))
replace_words.append(("Cal�", "Calò"))
replace_words.append(("Maff�", "Maffè"))
replace_words.append(("D�Angeli","D'Angeli"))
replace_words.append(("Al�", "Alì"))
replace_words.append(("Fig� Talamanca", "Figà Talamanca"))
replace_words.append(("Danz�", "Danzì"))
replace_words.append(("Filippo De Gasperi", "Filippo Degasperi"))
replace_words.append(("Noi con l'ItaliaUSEIRinascimento-AdC",
                     "Noi con l'Italia - USEI - Rinascimento - Alleanza di Centro"))
replace_words.append(("Noi con l?Italia-USEI-Cambiamo!-Alleanza di Centro","Noi con l'Italia - USEI - Cambiamo! - Alleanza di Centro"))
replace_words.append(("Noi con l?Italia-USEI-Cambiamo!-Alleanza di Centro",
                     "Noi con l'Italia - USEI - Cambiamo! - Alleanza di Centro"))


fields = ['ARGOMENTO','MICRO_CATEGORIA','PROGRAMMA','COGNOME','NOME']


# In[10]:


# apply cleanig
for field in fields:
    for words in replace_words:
        try:
            data.loc[data[field].str.contains(
                words[0]), field] = words[1]
        except Exception as e:
            print(words)
            pass


# In[ ]:


# DURATA as integer
data.DURATA = data.DURATA.astype(int)


# In[ ]:


def convertime(datestring):
    rdatestring = None
    if "." in datestring:
        rdatestring = pd.to_datetime(datestring,  format="%d.%m.%Y")
    elif "/" in datestring:
        rdatestring = pd.to_datetime(datestring,  format="%d/%m/%Y")
    return(rdatestring)


# In[ ]:


data['DATA'] = data['DATA'].apply(lambda x: x.replace(" 00:00", ""))
data['DATA'] = data['DATA'].apply(lambda x: x.replace(":00", ""))
data['DATA'] = data.DATA.apply(convertime) # = pd.to_datetime(data["DATA"],  format="%d.%m.%Y")


# In[ ]:


cols = {"CHANNEL":"channel",
        "PROGRAMMA":"program",
        "DATA":"day",
        "COGNOME":"lastname",
        "NOME":"name",
        "MICRO_CATEGORIA":"subcategory",
        "ARGOMENTO":"topic",
        "DURATA":"duration",
        "TIPO_TEMPO":"kind"}
data.rename(columns=cols,inplace=True)


# #

# In[ ]:


# save data
data.to_parquet(".." + os.sep + "docs"  + os.sep +  "data" + os.sep + "agcomdata.parquet")

