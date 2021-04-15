from bs4 import BeautifulSoup
import requests

resp = requests.get("https://na.leagueoflegends.com/en-us/champions/")
soup = BeautifulSoup(resp.content, 'html.parser')
champ_text_spans = soup.find_all("span", {"class": "style__Text-sc-12h96bu-3"})
print("EntityName,SubReddit")
for sp in champ_text_spans:
    print(f"{sp.text.lower()},leagueoflegends")