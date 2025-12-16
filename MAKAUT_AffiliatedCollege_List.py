
# open the MAKAUT affiliated college list page and scrape the data into an excel file with columns: College Code, College Name, College Website URL
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

URL = "https://makautwb.ac.in/page.php?id=363"

# ---------- Chrome Options ----------
options = uc.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-dev-shm-usage")

driver = uc.Chrome(options=options)
wait = WebDriverWait(driver, 30)

data = []

try:
    driver.get(URL)

    # Wait for table body
    tbody = wait.until(
        EC.presence_of_element_located((By.TAG_NAME, "tbody"))
    )

    rows = tbody.find_elements(By.TAG_NAME, "tr")

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) < 3:
            continue

        sr_no = cols[0].text.strip()
        college_code = cols[1].text.strip()

        college_name = ""
        college_url = ""

        # College Name + URL handling
        try:
            anchor = cols[2].find_element(By.TAG_NAME, "a")
            college_name = anchor.text.replace("\n", " ").strip()
            college_url = anchor.get_attribute("href")
        except:
            # No anchor tag
            college_name = cols[2].text.replace("\n", " ").strip()
            college_url = ""

        data.append({
            "Sr No": sr_no,
            "College Code": college_code,
            "College Name": college_name,
            "College Website URL": college_url
        })

    # ---------- Save to Excel ----------
    df = pd.DataFrame(data)
    df.to_excel("MAKAUT_College_List.xlsx", index=False)

    print("✅ Scraping completed successfully")
    print("📁 File saved as: MAKAUT_College_List.xlsx")

finally:
    driver.quit()
