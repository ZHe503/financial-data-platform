import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging


class FinancialDataPipeline:

    def __init__(self, tickers, period="1y", db_path="financial_data.db"):

        self.tickers = tickers
        self.period = period

        # Create SQLite engine
        self.engine = create_engine(f"sqlite:///{db_path}")

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def extract(self, ticker):
        # Download historical data for a ticker.
        try:
            logging.info(f"Extracting {ticker}")
            df = yf.download(ticker, period=self.period, auto_adjust=False)

            if df.empty:
                raise ValueError(f"No data returned for {ticker}")

            df["ticker"] = ticker
            return df

        except Exception as e:
            logging.error(f"Extraction failed for {ticker}: {e}")
            return None

    def transform(self, df):
        # Add returns and rolling volatility

        try:
            # ---- FIX MULTIINDEX COLUMNS ----
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Now safe to compute returns
            price_column = "Adj Close" if "Adj Close" in df.columns else "Close"

            df["return"] = df[price_column].pct_change()
            df["volatility_30"] = df["return"].rolling(30).std()

            df = df.dropna()
            return df

        except Exception as e:
            logging.error(f"Transformation failed: {e}")
            return None

    def load(self, df):
        # Load data into SQLite database.
        try:
            df.to_sql(
                "stock_prices",
                self.engine,
                if_exists="replace",
                index=True
            )

            logging.info("Data successfully loaded")

        except SQLAlchemyError as e:
            logging.error(f"Load failed: {e}")

    def run(self):

        for ticker in self.tickers:

            df = self.extract(ticker)
            if df is None:
                continue

            df = self.transform(df)
            if df is None:
                continue

            self.load(df)

        logging.info("Pipeline completed successfully.")

if __name__ == "__main__":

    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
               "NVDA", "META", "JPM", "V", "SPY"]

    pipeline = FinancialDataPipeline(tickers, period="1y")
    pipeline.run()
