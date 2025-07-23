
import yookassa
from yookassa import Payment
import uuid
from config import YOOKASSA_SECRET_KEY, YOOKASSA_SHOP_ID

yookassa.Configuration.account_id = YOOKASSA_SHOP_ID
yookassa.Configuration.secret_key = YOOKASSA_SECRET_KEY

async def create_payment(amount, chat_id):
    id_key = str(uuid.uuid4())
    payment = Payment.create({
        "amount": {
            'value': amount,
            'currency': "RUB",
        },
        'payment_method_data': {
            'type': 'bank_card'
        },
        'confirmation': {
            'type': 'redirect',
            'return_url': 'https://t.me/momentProfit_bot'
        },
        'capture': True,
        'metadata': {
            'chat_id': chat_id
        },
        'desctiption': 'Описание...'
    }, id_key)

    return payment.confirmation.confirmation_url, payment.id
