"""edit my cool new table

Revision ID: 1097f31cdffb
Revises: b4fee22b4a4d
Create Date: 2025-06-27 09:24:38.896851

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1097f31cdffb'
down_revision: Union[str, None] = 'b4fee22b4a4d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('core_zenodo_logs', sa.Column('software_hash_id', sa.String(), nullable=True, comment='A Software Heritage Software Hash ID (SWHID).'))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('core_zenodo_logs', 'software_hash_id')
    # ### end Alembic commands ###
