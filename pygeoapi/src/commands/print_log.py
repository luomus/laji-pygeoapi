from src.app import app
from src.models import RequestLog
from sqlalchemy import desc
import click
from tabulate import tabulate


@app.cli.command('print_log')
@click.argument('limit', required=False, type=int, default=100)
def print_log(limit):
    results = RequestLog.query.order_by(desc(RequestLog.date)).limit(limit).all()

    table = [['Status', 'Date', 'Api key', 'Path', 'Query', 'Ip address']]
    for row in reversed(results):
        table.append(
            [row.status_code, row.date, row.api_key_id, row.path, row.query_string, row.ip_address]
        )

    print(tabulate(table))