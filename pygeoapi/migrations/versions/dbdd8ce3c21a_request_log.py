"""empty message

Revision ID: dbdd8ce3c21a
Revises: 258549f11f9d
Create Date: 2024-11-01 12:42:06.709824

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dbdd8ce3c21a'
down_revision = '258549f11f9d'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('request_log',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('status_code', sa.Integer(), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=False),
    sa.Column('api_key_id', sa.Integer(), nullable=True),
    sa.Column('path', sa.Text(), nullable=False),
    sa.Column('query_string', sa.Text(), nullable=False),
    sa.Column('ip_address', sa.String(length=39), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('request_log')
    # ### end Alembic commands ###