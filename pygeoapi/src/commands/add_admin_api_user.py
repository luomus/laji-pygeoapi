from src.app import app, db
from src.models import AdminAPIUser
import click
import secrets


@app.cli.command('add_admin_api_user')
@click.argument('system_id', required=True)
def add_admin_api_user(system_id):
    user = AdminAPIUser.query.filter(
        AdminAPIUser.system_id == system_id
    ).first()

    if user is not None:
        raise ValueError('User with that system id exists already')

    password = secrets.token_urlsafe(48)

    user = AdminAPIUser(system_id=system_id, password=password)
    db.session.add(user)
    db.session.commit()

    print('Created user with password {}'.format(password))
