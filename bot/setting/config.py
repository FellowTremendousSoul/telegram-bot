from environs import Env


env = Env()
env.read_env()

BOT_TOKEN = env.str("BOT_TOKEN")

server_db = env.str("server_db")
db = env.str("db")
login_db = env.str("login_db")
password_db = env.str("password_db")


