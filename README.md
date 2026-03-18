# 🔍 Detector de Duplicidades de Ordens de Serviço

Aplicação em **Python + Streamlit** para identificar **ordens de serviço duplicadas** para o mesmo cliente, considerando o mesmo tipo de serviço em uma **janela de tempo configurável**.

A ferramenta lê planilhas (Excel ou CSV), permite configurar quais colunas representam cliente, tipo de serviço, data e número da OS, e gera um relatório detalhado com:

- A **OS original** (primeira abertura) para cada grupo
- As **OS duplicadas** abertas depois, dentro do período configurado
- Resumo por cliente/serviço e exportação para Excel com formatação

---

## ✨ Principais funcionalidades

- **Upload de planilhas**
  - Suporte a arquivos `.xlsx`, `.xls` e `.csv`
  - Leitura robusta das colunas em formato texto

- **Configuração flexível das colunas**
  - Seleção da coluna que identifica o **cliente**  
    (ex.: `cliente`, `matricula`, `cpf`, `id_cli`, etc.)
  - Seleção da coluna de **tipo de serviço**  
    (ex.: `serviço`, `tipo_servico`, `categoria`, etc.)
  - Seleção da coluna de **data da OS**
  - Coluna opcional de **número da OS** para enriquecer o relatório
  - Seleção de **colunas adicionais** para aparecerem nos resultados

- **Tratamento robusto de datas**
  - Função de parsing que tenta vários formatos comuns (`DD/MM/YYYY`, `DD/MM/YYYY HH:MM`, `YYYY-MM-DD`, etc.)
  - Diagnóstico em tela mostrando:
    - Quantos registros tiveram a data reconhecida
    - Quantos foram ignorados por formato inválido
    - Exemplos de datas não reconhecidas, se houver

- **Lógica de duplicidade (cliente + serviço + tempo)**
  - Para cada combinação de **cliente + tipo de serviço**:
    - As OS são ordenadas por data
    - A **primeira OS** em um cluster de datas próximas é tratada como **ORIGINAL (legítima)**
    - As demais OS abertas dentro da **janela de dias configurada** são marcadas como **DUPLICATAS**
  - Dois níveis de saída:
    - **Detalhamento**: uma linha para cada OS (original ou duplicata)
    - **Resumo por grupo**: uma linha por grupo de duplicidades

- **Filtro de período (opcional)**
  - Possibilidade de restringir a análise a um intervalo de datas
  - Exemplo: analisar apenas OS entre `01/01/2024` e `31/12/2024`

- **Visualização dentro do app**
  - Pré-visualização das primeiras linhas do arquivo de entrada
  - Tabelas interativas:
    - **Detalhamento das duplicidades** (OS original + duplicatas)
    - **Resumo por grupo** (cliente, tipo de serviço, datas e quantidades)
  - Métricas principais na tela, como:
    - Número de grupos de duplicidade
    - Total de OS duplicadas
    - Quantidade de tipos de serviço com duplicidades
    - Janela de tempo utilizada

- **Exportação para Excel**
  - Geração de arquivo `.xlsx` com 3 abas:
    1. **Duplicidades**  
       - OS ORIGINAL destacada em **verde claro**  
       - OS DUPLICATAS coloridas por grupo
       - Colunas configuradas pelo usuário (cliente, serviço, data, OS, extras)
    2. **Resumo por Grupo**  
       - Cliente, tipo de serviço  
       - Data e número da OS original (quando informado)  
       - Quantidade de duplicatas  
       - Datas da primeira e da última duplicata  
       - Intervalo de dias entre duplicatas
    3. **Configurações**  
       - Data/hora da geração  
       - Janela de análise utilizada (em dias)  
       - Total de grupos encontrados  
       - Total de OS duplicadas

---

## 🧰 Tecnologias utilizadas

- [Python](https://www.python.org/)
- [Streamlit](https://streamlit.io/) — interface web
- [pandas](https://pandas.pydata.org/) — tratamento de dados
- [openpyxl](https://openpyxl.readthedocs.io/) — geração e formatação de Excel

---

## 📦 Instalação

1. **Criar/ativar ambiente virtual** (opcional, mas recomendado):

```bash
python -m venv .venv
# Windows:
.\.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate
```

2. **Instalar dependências**:

```bash
pip install streamlit pandas openpyxl xlrd
```

> Observação: `xlrd` é usado apenas para compatibilidade com alguns formatos antigos de Excel (`.xls`).

---

## ▶️ Como executar o app

Salve o código do aplicativo em um arquivo, por exemplo:

```text
duplicidades_app.py
```

No terminal, dentro da pasta do arquivo, execute:

```bash
streamlit run duplicidades_app.py
```

O Streamlit abrirá o app automaticamente no navegador (ou mostrará o endereço local, geralmente `http://localhost:8501`).

---

## 🧮 Como funciona a lógica de duplicidade

1. **Carregamento e limpeza**
   - Lê o arquivo enviado e converte todas as colunas para texto.
   - Remove espaços extras dos nomes das colunas.
   - Converte a coluna de data com a função `parse_dates_robust`, que testa múltiplos formatos.

2. **Normalização**
   - Cria colunas auxiliares normalizadas:
     - Cliente em maiúsculas e sem espaços extras
     - Tipo de serviço em maiúsculas e sem espaços extras

3. **Agrupamento**
   - Agrupa o dataset por `(cliente, tipo de serviço)`.

4. **Janela deslizante de datas**
   - Dentro de cada grupo:
     - Ordena por data de OS
     - Percorre a lista de datas com uma janela deslizante:
       - Para cada OS, procura as próximas OS dentro de `N` dias (janela configurada)
       - Se encontrar mais de uma OS na janela:
         - A primeira é marcada como **ORIGINAL**
         - As demais da janela são marcadas como **DUPLICATAS**
     - Garante que a mesma linha não entre em dois grupos diferentes.

5. **Geração dos resultados**
   - **Detalhamento**:  
     Uma linha por OS, com:
     - `grupo_duplicidade`
     - `tipo_registro` (`ORIGINAL` ou `DUPLICATA`)
     - Cliente, tipo de serviço, data, número da OS (se houver) e colunas extras
   - **Resumo por grupo**:
     - Cliente, tipo de serviço
     - Data da OS original e, opcionalmente, número da OS original
     - Quantidade de duplicatas
     - Datas da primeira e da última duplicata
     - Intervalo de dias entre duplicatas

---

## 🖥️ Uso básico

1. Abra o app no navegador.
2. Faça upload de um arquivo `.xlsx`, `.xls` ou `.csv`.
3. Confirme/ajuste:
   - Coluna de cliente
   - Coluna de tipo de serviço
   - Coluna de data
   - (Opcional) Coluna de número da OS
   - (Opcional) Colunas extras para aparecerem no resultado
4. Defina a **janela de tempo** em dias (ex.: 30 dias).
5. (Opcional) Ative o filtro de período e escolha data inicial/final.
6. Clique em **“Analisar duplicidades”**.
7. Consulte as abas:
   - “Detalhamento das duplicidades”
   - “Resumo por grupo”
8. Baixe o relatório Excel clicando em **“Baixar relatório XLSX”**.

---

## ✅ Considerações e boas práticas

- Verifique se a coluna de data está realmente no formato esperado (principalmente se vier de sistemas diferentes).
- Datas que não puderem ser interpretadas serão ignoradas na análise, mas o app mostra quantas são e alguns exemplos.
- Se você perceber que algum formato de data específico da sua base não está sendo reconhecido, é possível adicionar novos formatos à função `parse_dates_robust`.

---

## 📄 Licença

(Defina aqui a licença do projeto, por exemplo: MIT, Apache 2.0, uso interno, etc.)

---

Se quiser, posso montar também um README separado para o outro app (o populador de campanha CORSAN com o template XLSX). Quer que eu faça esse segundo README também?

