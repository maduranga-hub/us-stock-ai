# 🚀 US Stock AI: Quantitative Trader

A professional-grade automated stock monitoring system tailored for the Dubai (GST) timezone. Scans US Large-Cap stocks (> $10B) using 1-Hour RSI oversold strategy with SMA trend confirmation and Bullish Fair Value Gap (FVG) detection.

## 🛠️ Features
- **1H Strategy**: RSI <= 35 + Price > SMA 100.
- **FVG Detection**: Detects Bullish Fair Value Gaps for entry confirmation.
- **Automated Alerts**: Real-time Telegram notifications with Dubai timestamp and Market Status.
- **Premium Dashboard**: Cyber-Glassmorphism UI built with Streamlit.
- **Serverless Execution**: GitHub Actions automation running 24/5.

## 📁 Project Structure
- `main.py`: Core scanner logic and indicator calculations.
- `app.py`: Streamlit dashboard with interactive Plotly charts.
- `requirements.txt`: Python dependencies.
- `.github/workflows/daily.yml`: Automation schedule (11:30 - 21:30 UTC).

## 🚀 Setup & Deployment

### 1. Telegram Bot Setup
- Create a bot via [@BotFather](https://t.me/botfather).
- Get your `CHAT_ID` via [@userinfobot](https://t.me/userinfobot).

### 2. Local Setup
1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file from `.env.example` and add your keys.
4. Run the dashboard:
   ```bash
   streamlit run app.py
   ```

### 3. GitHub Actions (Automation)
1. Push this code to a Private/Public GitHub repository.
2. Go to **Settings > Secrets and variables > Actions**.
3. Add the following repository secrets:
   - `TELEGRAM_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - `DASHBOARD_URL` (Link to your Streamlit cloud deployment)

### 4. Streamlit Cloud (Dashboard)
1. Connect your GitHub repo to [Streamlit Cloud](https://share.streamlit.io/).
2. Add your secrets to the Streamlit app settings.

---
**Status**: 🟢 PRODUCTION READY  
**Version**: 1.0 (Dubai Edition)
