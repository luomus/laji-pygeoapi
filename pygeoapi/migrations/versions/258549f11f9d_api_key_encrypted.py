"""empty message

Revision ID: 258549f11f9d
Revises: cd9cf91b69c7
Create Date: 2024-10-09 09:18:47.607098

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '258549f11f9d'
down_revision = 'cd9cf91b69c7'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('DELETE FROM api_key')
    with op.batch_alter_table('api_key', schema=None) as batch_op:
        batch_op.add_column(sa.Column('key_encrypted', sa.String(length=200), nullable=False))
        batch_op.drop_column('key_hash')


def downgrade():
    op.execute('DELETE FROM api_key')
    with op.batch_alter_table('api_key', schema=None) as batch_op:
        batch_op.add_column(sa.Column('key_hash', sa.String(length=162), nullable=False))
        batch_op.drop_column('key_encrypted')
