import asyncio
import csv
import json
import logging
import random
import re

from playwright.async_api import async_playwright

logging.basicConfig(
    filename="vagas.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

logger = logging.getLogger()


async def catho_scraper():
    csv_file_path = "vagas.csv"
    json_file_path = "vagas.json"
    vagas_list = []

    try:
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(
                [
                    "Título",
                    "Link",
                    "Local",
                    "Salário",
                    "Salário Anunciado",
                    "Fonte",
                    "Salario_Inf",
                    "Salario_Sup",
                ]
            )
        logger.info(f"Arquivo CSV '{csv_file_path}' criado com cabeçalho.")
    except Exception as e:
        logger.error(f"Erro ao criar o CSV com cabeçalho: {str(e)}")
        return

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
            )
            page = await context.new_page()
            logger.info("Navegador iniciado com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao iniciar o navegador: {str(e)}")
            return

        async def goto_with_retries(page, url, retries=3, wait=5):
            for attempt in range(1, retries + 1):
                try:
                    await page.goto(url, timeout=60000)
                    return
                except Exception as e:
                    if attempt == retries:
                        raise
                    else:
                        logger.error(f"Erro ao acessar {url}: {str(e)}")
                        logger.info(
                            f"Tentativa {attempt} de {retries}. Aguardando {wait} segundos antes de tentar novamente."
                        )
                        await asyncio.sleep(wait)

        try:
            page_number = 1
            max_pages = 200
            while page_number <= max_pages:
                url = f"https://www.catho.com.br/vagas/?page={page_number}"

                try:
                    await goto_with_retries(page, url)
                except Exception as e:
                    logger.error(
                        f"Erro irreparável ao acessar {url} após múltiplas tentativas: {str(e)}"
                    )
                    logger.info("Finalizando o scraping.")
                    break

                try:
                    await page.wait_for_selector("//ul/li/article", timeout=15000)
                except Exception as e:
                    logger.error(
                        f"Timeout ao esperar os elementos de vagas na página {page_number}: {str(e)}"
                    )
                    break

                await page.wait_for_timeout(5000)

                logger.info(f"\n=== Acessando página {page_number}: {url} ===")

                try:
                    vagas_elements = await page.query_selector_all("//ul/li/article")
                    logger.info(
                        f"Total de vagas encontradas na página {page_number}: {len(vagas_elements)}"
                    )
                except Exception as e:
                    logger.error(
                        f"Erro ao selecionar elementos de vagas na página {page_number}: {str(e)}"
                    )
                    break

                if not vagas_elements:
                    logger.info(
                        "Nenhuma vaga encontrada nesta página. Finalizando scraping."
                    )
                    break
                else:
                    logger.info(
                        f"Total de vagas encontradas na página {page_number}: {len(vagas_elements)}"
                    )

                for idx, vaga_element in enumerate(vagas_elements, start=1):
                    try:
                        logger.info(
                            f"\nProcessando vaga {idx} na página {page_number}."
                        )

                        titulo_element = await vaga_element.query_selector(
                            "h2 a[title]"
                        )
                        titulo_raw = (
                            await titulo_element.get_attribute("title")
                            if titulo_element
                            else "Título não encontrado"
                        )
                        link = (
                            await titulo_element.get_attribute("href")
                            if titulo_element
                            else "Link não encontrado"
                        )

                        titulo_sanitizado = re.sub(
                            r"^Vaga\s+de\s+(.+?)(\s+em\s+.*|[-#].*|$)",
                            r"\1",
                            titulo_raw,
                            flags=re.IGNORECASE,
                        ).strip()

                        logger.info(f"Título Original: {titulo_raw}")
                        logger.info(f"Título Sanitizado: {titulo_sanitizado}")
                        logger.info(f"Link: {link}")

                        local_element = await vaga_element.query_selector(
                            'div > button > a[href*="/vagas/"]'
                        )
                        if not local_element:
                            local_element = await vaga_element.query_selector(
                                'div > div > a[href*="/vagas/"]'
                            )
                        local_text = (
                            await local_element.inner_text()
                            if local_element
                            else "Local não encontrado"
                        )

                        salario_element = await vaga_element.query_selector(
                            'div > div[class*="salaryText"]'
                        )
                        salario_text = (
                            await salario_element.inner_text()
                            if salario_element
                            else "Salário não encontrado"
                        )

                        salario_anunciado = not re.search(
                            r"a combinar|não informado|não divulgado",
                            salario_text,
                            re.IGNORECASE,
                        )

                        salario_inf = 0.0
                        salario_sup = 0.0
                        if salario_anunciado:
                            salarios = re.findall(
                                r"R\$\s?([\d\.]+,\d{2})", salario_text
                            )
                            if salarios:
                                salarios = [
                                    float(s.replace(".", "").replace(",", "."))
                                    for s in salarios
                                ]
                                if len(salarios) == 1:
                                    salario_inf = salarios[0]
                                    salario_sup = salarios[0]
                                elif len(salarios) >= 2:
                                    salario_inf = salarios[0]
                                    salario_sup = salarios[1]

                        logger.info(f"Local: {local_text}")
                        logger.info(f"Salário: {salario_text}")
                        logger.info(f"Salário Anunciado: {salario_anunciado}")
                        logger.info(f"Salário Inferior: {salario_inf}")
                        logger.info(f"Salário Superior: {salario_sup}")

                        vaga_info = {
                            "titulo": titulo_sanitizado,
                            "link": link.strip() if link else "Link não encontrado",
                            "local": local_text.strip(),
                            "salario": salario_text.strip(),
                            "salario_anunciado": salario_anunciado,
                            "fonte": url,
                            "salario_inf": salario_inf,
                            "salario_sup": salario_sup,
                        }

                        try:
                            with open(
                                csv_file_path, "a", newline="", encoding="utf-8"
                            ) as csvfile:
                                csvwriter = csv.writer(csvfile)
                                csvwriter.writerow(
                                    [
                                        vaga_info["titulo"],
                                        vaga_info["link"],
                                        vaga_info["local"],
                                        vaga_info["salario"],
                                        vaga_info["salario_anunciado"],
                                        vaga_info["fonte"],
                                        vaga_info["salario_inf"],
                                        vaga_info["salario_sup"],
                                    ]
                                )
                            logger.info(f"Vaga {idx} registrada no CSV.")
                        except Exception as e:
                            logger.error(
                                f"Erro ao escrever a vaga {idx} no CSV: {str(e)}"
                            )

                        vagas_list.append(vaga_info)

                    except Exception as e:
                        logger.error(
                            f"Erro ao processar a vaga {idx} na página {page_number}: {str(e)}"
                        )

                delay = random.uniform(1, 3)
                await page.wait_for_timeout(delay * 1000)
                page_number += 1

            logger.info("Exportando os dados coletados para JSON.")
            try:
                with open(json_file_path, "w", encoding="utf-8") as jsonfile:
                    json.dump(vagas_list, jsonfile, ensure_ascii=False, indent=4)
                logger.info("JSON criado com sucesso.")
            except Exception as e:
                logger.error(f"Erro ao criar o JSON: {str(e)}")

        except Exception as e:
            logger.error(f"Erro durante o scraping: {str(e)}")
        finally:
            await browser.close()
            logger.info("Navegador fechado.")


if __name__ == "__main__":
    asyncio.run(catho_scraper())
