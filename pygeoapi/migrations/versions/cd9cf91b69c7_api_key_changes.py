"""api_key_changes

Revision ID: cd9cf91b69c7
Revises: d9b9377cf7d5
Create Date: 2024-10-01 10:10:33.636070

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'cd9cf91b69c7'
down_revision = 'd9b9377cf7d5'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('DELETE FROM api_key')

    with op.batch_alter_table('api_key', schema=None) as batch_op:
        batch_op.add_column(sa.Column('created', sa.DateTime(), nullable=False))
        batch_op.add_column(sa.Column('expires', sa.DateTime(), nullable=False))
        batch_op.add_column(sa.Column('data_use_purpose', sa.Text(), nullable=False))
        batch_op.drop_column('expire_date')
        batch_op.drop_column('created_date')


def downgrade():
    op.execute('DELETE FROM api_key')

    with op.batch_alter_table('api_key', schema=None) as batch_op:
        batch_op.add_column(sa.Column('created_date', sa.DateTime(), nullable=False))
        batch_op.add_column(sa.Column('expire_date', sa.DateTime(), nullable=False))
        batch_op.drop_column('data_use_purpose')
        batch_op.drop_column('expires')
        batch_op.drop_column('created')
