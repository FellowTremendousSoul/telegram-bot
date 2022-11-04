from environs import Env

env = Env()
env.read_env()

ADMINS = env.list("admin_id")  # Тут у нас будет список из админов
CONTACT_PHONE = ["+79875373042", "+79299973206"]

