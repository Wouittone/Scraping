"""

    https://towardsdatascience.com/data-science-skills-web-scraping-javascript-using-python-97a29738353f

"""

import asyncio
import orjson as json
import logging
import random
import re
from time import perf_counter as counter
from typing import Optional

from arsenic import Session, browsers, get_session, services
from arsenic.session import Element
from arsenic.errors import NoSuchElement
import tenacity as tnct

URL = "https://store.citroen.fr/trim/stock/c1-berline-5-portes/VPAC0031/00000009?energies=Electrique"

RESULTS: dict

LOGGING_FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

LOGGING_PATH = "./Scraping ëC4.log"

# Make sure the .log file is emptyied before each run
fh = open(LOGGING_PATH, "w")
fh.close()

LOGGING_HANDLER = logging.FileHandler(LOGGING_PATH)
LOGGING_HANDLER.setLevel(logging.DEBUG)
LOGGING_HANDLER.setFormatter(LOGGING_FORMATTER)


def get_logger(name: str) -> logging.Logger:
    logger: logging.Logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(LOGGING_HANDLER)
    return logger


async def get_car_nb(logger: Optional[logging.Logger] = None) -> int:
    logger = get_logger("Init")

    service = services.Geckodriver()
    browser = browsers.Firefox(
        **{"moz:firefoxOptions": {"args": ["-headless", "-log", "error"]}}
    )

    async with get_session(service, browser) as session:

        await session.get(URL)
        await asyncio.sleep(5 + random.random())

        # Accept cookies
        logger.info(f"Accepting cookies for init run")
        accept_all = await session.get_element("#_psaihm_id_accept_all_btn")
        await accept_all.click()

        nb_cars_we = await session.get_element(".offer-list--vehicles-count b")
        nb_cars_str = await nb_cars_we.get_text()

        nb_cars = int(re.search("(\\d+)", nb_cars_str).group())

        logger.info(f"Found {nb_cars} cars to be parsed on init run")

    del browser, service

    return nb_cars


async def parse_header(session: Session):
    model_we = await session.get_element(".car-info-model-label")
    finition = await model_we.get_text()

    price_we = await session.get_element("span.cashContainer-cashPrice-price")
    formatted_price = await price_we.get_text()

    dt_we = await session.get_element(".financeContainer-leadTime-date time")
    formatted_date = await dt_we.get_attribute("datetime")

    return {
        "FINITION": finition,
        "PRICE": formatted_price,
        "VALIDITY": formatted_date,
    }


async def parse_main_block(session: Session):
    results = {}

    wrapper_wes = await session.get_elements(
        ".technicalDetails-mainBlock > .technicalWrapper"
    )
    for wrapper_we in wrapper_wes:
        title_we = await wrapper_we.get_element(".featureTitleWide")
        title = await title_we.get_text()

        inner_res = {}
        for tfmw_we in await wrapper_we.get_elements(".technicalFeaturesModuleWrap"):
            fl_we = await tfmw_we.get_element(".featureList")
            fl = await fl_we.get_text()

            fil_we = await tfmw_we.get_element(".featureItemList")
            fil = await fil_we.get_text()

            inner_res[fl] = fil

        results[title] = inner_res

    return results


async def parse_technical_wrappers_list(sb: Element):
    results = {}

    for tw in await sb.get_elements(".technicalWrapper"):
        we_title = await tw.get_element(".featureTitle")
        title = await we_title.get_text()

        inner_res = {}
        for li in await tw.get_elements(".featureList li"):
            li_txt = await li.get_text()
            inner_res[li_txt] = True

        results[title] = inner_res

    return results


async def parse_technical_wrappers_dict(sb: Element):
    results = {}

    for tw in await sb.get_elements(".technicalWrapper"):
        ew_title = await tw.get_element(".featureTitle")
        title = await ew_title.get_text()

        inner_res = {}
        for li in await tw.get_elements("ul li"):
            we_fi = await li.get_element(".featureInfo")
            fi = await we_fi.get_text()

            we_fv = await li.get_element(".featureValue")
            fv = await we_fv.get_text()

            inner_res[fi] = fv

        results[title] = inner_res

    return results


async def parse_specification_blocks(session: Session):
    debug_logger = get_logger("DEBUG")
    debug_logger.setLevel(logging.DEBUG)

    sblocks = await session.get_elements(".characteristicsGrid .specificationBlock")

    res_list = await parse_technical_wrappers_list(sblocks[0])
    res_dict = await parse_technical_wrappers_dict(sblocks[1])

    return res_list | res_dict


async def parse_car_page(session):
    header = await parse_header(session)
    mblock = await parse_main_block(session)
    sblock = await parse_specification_blocks(session)
    return header | mblock | sblock


async def get_pages_list(session: Session):
    return await session.get_elements(".showMore li")


async def get_pages_we_numbers(wes: list[Element]):
    wes_txts = [await we.get_text() for we in wes]
    return [int(txt) for txt in wes_txts if txt.isnumeric()]


async def click_last_break_me(wes: list[Element]):
    brks = [p for p in wes[::-1] if await p.get_attribute("class") == "break-me"]
    if brks:
        await brks[0].click()


async def go_to_page_number(session, page_number):

    pages = await get_pages_list(session)
    needed_page_is_visible = page_number in await get_pages_we_numbers(pages)
    while not needed_page_is_visible:
        pages = await get_pages_list(session)
        await click_last_break_me(pages)
        await asyncio.sleep(1 + 0.5 * random.random())

        pages = await get_pages_list(session)
        needed_page_is_visible = page_number in await get_pages_we_numbers(pages)

    pages = await get_pages_list(session)
    we_to_clicks = [page for page in pages if await page.get_text() == str(page_number)]
    we_to_click = we_to_clicks[0]
    if await we_to_click.get_attribute("class") != "active":
        await we_to_click.click()
    await asyncio.sleep(2 + 0.5 * random.random())


@tnct.retry(
    retry=tnct.retry_if_exception_type(NoSuchElement),
    stop=tnct.stop_after_attempt(3),
    wait=tnct.wait_exponential(multiplier=1, min=4, max=10),
)
async def get_to_car_characteristics(session, logger: logging.Logger) -> None:
    get_car_info_btn = await session.get_element(".car-info-characteristics-button")
    await get_car_info_btn.click()
    await asyncio.sleep(2 + 0.2 * random.random())


async def get_car_info_at_pg_cell_tpl(logger, page_number, cell_number):
    try:
        # Create a new headless browser instance
        logger.info(f"Creating browser for page {page_number} / cell {cell_number}")

        service = services.Geckodriver()
        browser = browsers.Firefox(
            **{"moz:firefoxOptions": {"args": ["-headless"], "log": {"level": "error"}}}
        )

        async with get_session(service, browser) as session:
            start = counter()

            await session.get(URL)
            await asyncio.sleep(5 + random.random())

            # Accept cookies
            logger.info(
                f"Accepting cookies for page {page_number} / cell {cell_number}"
            )
            accept_all = await session.get_element("#_psaihm_id_accept_all_btn")
            await accept_all.click()

            # Got to the requested page
            logger.info(
                f"Accessing page {page_number} (for page/cell ({page_number}, {cell_number}))..."
            )
            await go_to_page_number(session, page_number)

            # Click the requested cell
            logger.info(f"Accessing cell {cell_number} (on page {page_number})...")
            cells_btns = await session.get_elements(
                ".stockModelCard-dealerInfo--ctaSection"
            )
            choose_cell_btn = await cells_btns[cell_number - 1].get_element("button")
            await choose_cell_btn.click()
            await asyncio.sleep(4 + random.random())

            # Access the car info
            logger.info(f"Parsing page {page_number} / cell {cell_number}...")
            await get_to_car_characteristics(session, logger)

            # Parse the car info
            result = await parse_car_page(session)

            stop = counter()

            logger.info(
                f"Parsed page {page_number} / cell {cell_number} in {stop - start} s."
            )

            return result

    except Exception as e:
        logger.error(
            f"Error during parsing of page {page_number} / cell {cell_number}", e
        )
        return {"Exception": str(e)}


async def pg_cell_processor(ii, pgs_cells_q):
    logger = get_logger(f"Worker {str(ii + 1).rjust(2, '0')}")
    while True:
        pg_cell_tpl = await pgs_cells_q.get()

        result = await get_car_info_at_pg_cell_tpl(logger, *pg_cell_tpl)

        page_nb, cell_nb = pg_cell_tpl
        RESULTS[page_nb - 1]["CARS"][cell_nb - 1]["CHARACTERISTICS"] = result

        pgs_cells_q.task_done()


async def main(page_cell_tpls):
    pgs_cells_q = asyncio.Queue()

    for pg_cell_tpl in page_cell_tpls:
        pgs_cells_q.put_nowait(pg_cell_tpl)

    tasks = [
        asyncio.create_task(pg_cell_processor(ii, pgs_cells_q)) for ii in range(10)
    ]

    await pgs_cells_q.join()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    with open("Results ëC4.json", "wb") as fh:
        fh.write(json.dumps(RESULTS))


if __name__ == "__main__":
    logger = get_logger("Main")

    # Parse the URL a first time to get the total number of cars
    logger.info("Retrieving the number of cars to parse...")
    nb_cars = asyncio.run(get_car_nb())

    pc_tpls = [(ii // 9 + 1, ii % 9 + 1) for ii in range(nb_cars)]
    logger.info(
        f"{nb_cars} cars will be parsed from page/cell {pc_tpls[0]} to page/cell {pc_tpls[-1]}"
    )

    RESULTS = [
        {
            "PAGE": page + 1,
            "CARS": [
                {"CAR": car + 1, "CHARACTERISTICS": {}}
                for car in range(max(tpl[1] for tpl in pc_tpls if tpl[0] == page + 1))
            ],
        }
        for page in range(max(tpl[0] for tpl in pc_tpls))
    ]

    logger.info("Starting parsing...")
    start = counter()
    asyncio.run(main(pc_tpls))
    stop = counter()

    logger.info(f"Finished parsing {nb_cars} cars in {stop - start} s.")
