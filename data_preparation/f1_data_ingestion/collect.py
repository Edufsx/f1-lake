#%% 
import pandas as pd
import time
import argparse
import fastf1

pd.set_option('display.max_columns', None)

#%%

class CollectResults:
    """
    Classe responsável por coletar dados históricos das corridas de Fórmula 1
    utilizando a biblioteca FastF1.

    A classe permite:
    - Coletar dados por ano, corrida (GP) e tipo de sessão (Race, Sprint);
    - Salvar dados localmente em formato parquet;
    - Automatizar a coleta e salvamento de dados para múltiplos anos e sessões.

    Parâmetros:
    -----------
    - `years`: list[int] -> Lista de anos a serem coletados;

    - `modes`: list[str] -> Tipos de sessão:
        - "R": Race;
        - "S": Sprint.
    """
    
    def __init__(self, years=[2021, 2022, 2023], modes=["R", "S"]):
        
        self.years = years
        self.modes = modes

    def get_data(self, year:int, gp:int, mode:str) -> pd.DataFrame:
        """
        Coleta os resultados de uma sessão específica da Fórmula 1.

        A função utiliza a FastF1 para acessar dados históricos de uma corrida.
        Internamente, a biblioteca faz requisições a uma API e/ou lê dados cacheados.

        Parâmetros:
        -----------
        - `year`: int -> Ano da temporada;

        - `gp`: int -> Número do Grande Prêmio (GP);

        - `mode`: str -> Tipo de Sessão ("R" para Race e "S" para Sprint)

        Retorna:
        --------
        - pd.DataFrame -> DataFrame com os resultados da sessão 
        ou vazio em caso de erro.
        """

        try:
            session = fastf1.get_session(year, gp, mode)
        
        except ValueError as err:
            return pd.DataFrame()
        
        # Método interno da FastF1 para carregar resultados
        session._load_drivers_results()
        df = session.results

        # Adiciona informações contextuais da corrida
        df["Year"] = session.date.year
        df["Date"] = session.date
        df["Mode"] = session.name
        df["RoundNumber"] = session.event["RoundNumber"]
        df["OfficialEventName"] = session.event["OfficialEventName"]
        df["Country"] = session.event["Country"]
        df["Location"] = session.event["Location"]

        return df

    def save_data(self, df:pd.DataFrame, year:int, gp:int, mode:str):
        """
        Salva os dados coletados de um sessão de Fórmula 1 em formato parquet.

        O formato parquet é um formato de arquivo amplamente utilizado por permitir:
        - Compressão otimizada (menor uso de armazenamento);
        - Leitura rápida, especialmente para grandes volumes de dados.

        O nome do arquivo segue o padrão:
            {ano}\_{round}\_{modo}.parquet

        Parâmetros:
        -----------
        - `df`: pd.DataFrame -> Dados da sessão a serem salvos;

        - `year`: int -> Ano da temporada;

        - `gp`: int -> Número da corrida (round);

        - `mode`: str -> "R" (Race) ou "S" (Sprint).
        """
        
        file_name = f"../../data/raw/{year}_{gp:02}_{mode}.parquet"

        df.to_parquet(file_name, index=False)

    def process(self, year:int, gp:int, mode:str) -> bool:
        """
        Executa o pipeline completo para uma única sessão:
        1. Coleta os dados;
        2. Valida se há dados;
        3. Salva no pasta "data".

        Parâmetros:
        -----------
        - `year`: int -> Ano da temporada;

        - `gp`: int -> Número da corrida (round);

        - `mode`: str -> "R" (Race) ou "S" (Sprint).

        Retorna:
        --------
        - bool -> True, se processado com sucesso, 
        False, caso contrário
        """
        
        df = self.get_data(year, gp, mode)

        if df.empty:
            return False
        
        self.save_data(df, year, gp, mode)
        
        # Um segundo de pausa para evitar sobrecarga de requisições 
        time.sleep(1)

        return True
    
    def process_year_modes(self, year):
        """
        Executa a coleta de dados para todas as corridas (GPs) de um determinado ano,
        considerando os diferentes tipos de sessão definidos.

        A função percorre sequencialmente os possíveis rounds da temporada e,
        para cada um, tenta coletar os dados para os modos especificados.

        Parâmetros:
        -----------
        `year`: int -> Ano da Temporada a ser processada
        """
        
        # Itera sobre um número máximo esperado de GPs em uma temporada
        for i in range(1, 30):
           
            # Itera sobre os tipos de sessão (Race, Sprint)
            for mode in self.modes:
                
                # Salva dados dos GPs e dos modos no ano selecionado
                # Se não houver mais GPs do tipo Race no ano selecionado, interrompe o loop
                if not self.process(year, i, mode) and mode == "R": 
                    break
    
    def process_years(self):
        """
        Orquestra a execução da coleta de dados para múltiplos anos.

        Para cada ano definido em `self.years`, a função executa o pipeline
        de coleta de dados de todas as corridas (GPs) e modos de sessão.
        """

        # Itera sobre o anos definidos para coleta
        for year in self.years:
            
            print(f"Coletando dados do ano de {year}...")
            
            # Executa o processamento completo de um ano (todos os GPs e modos)
            self.process_year_modes(year)

            # Pausa entre anos para evitar sobrecarga da API
            time.sleep(10)
    
#%%

# --- Interface de linha de comando (CLI) para execução do pipeline de coleta de dados. ---
"""
Permite ao usuário definir:
- Intervalo de anos (via --start e --stop)
- Lista específica de anos (via --years)
- Tipos de sessão (via --modes)
"""

if __name__ == "__main__":
    
    # Inicializa o parser de argumentos da CLI
    parser = argparse.ArgumentParser()

    # Argumentos para definir intervalo de anos
    parser.add_argument("--start", type=int)
    parser.add_argument("--stop", type=int)

    # Argumentos para lista explícita de anos
    parser.add_argument("--years", "-y", nargs="+", type=int)

    # Lista com os tipos de sessão (Race, Sprint, etc.)
    parser.add_argument("--modes", "-m", nargs="+")

    args = parser.parse_args()

    # Prioriza lista explícita de anos, se fornecida
    if args.years:
        collect = CollectResults(args.years, args.modes)

    # Utiliza intervalo de anos, se definidos --start e --stop
    elif args.start and args.stop:
        years = [i for i in range(args.start, args.stop + 1)]
        collect = CollectResults(years, args.modes)

    # Executa o pipeline completo de dados
    collect.process_years()