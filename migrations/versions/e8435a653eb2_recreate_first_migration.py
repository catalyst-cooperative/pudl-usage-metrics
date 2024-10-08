"""Recreate first migration

Revision ID: e8435a653eb2
Revises:
Create Date: 2024-09-20 12:53:39.454455

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e8435a653eb2'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('core_github_clones',
    sa.Column('metrics_date', sa.Date(), nullable=False, comment='The date for each metrics snapshot.'),
    sa.Column('total_clones', sa.Integer(), nullable=True),
    sa.Column('unique_clones', sa.Integer(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('metrics_date')
    )
    op.create_table('core_github_forks',
    sa.Column('id', sa.Integer(), nullable=False, comment='The unique identifier for each fork.'),
    sa.Column('node_id', sa.String(), nullable=True),
    sa.Column('name', sa.String(), nullable=True),
    sa.Column('full_name', sa.String(), nullable=True),
    sa.Column('private', sa.Boolean(), nullable=True),
    sa.Column('owner', sa.String(), nullable=True),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('url', sa.String(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('pushed_at', sa.DateTime(), nullable=True),
    sa.Column('homepage', sa.String(), nullable=True),
    sa.Column('size_kb', sa.Integer(), nullable=True),
    sa.Column('stargazers_count', sa.Integer(), nullable=True),
    sa.Column('watchers_count', sa.Integer(), nullable=True),
    sa.Column('language', sa.String(), nullable=True),
    sa.Column('has_issues', sa.Boolean(), nullable=True),
    sa.Column('has_projects', sa.Boolean(), nullable=True),
    sa.Column('has_downloads', sa.Boolean(), nullable=True),
    sa.Column('has_wiki', sa.Boolean(), nullable=True),
    sa.Column('has_pages', sa.Boolean(), nullable=True),
    sa.Column('has_discussions', sa.Boolean(), nullable=True),
    sa.Column('forks_count', sa.Integer(), nullable=True),
    sa.Column('archived', sa.Boolean(), nullable=True),
    sa.Column('disabled', sa.Boolean(), nullable=True),
    sa.Column('license', sa.String(), nullable=True),
    sa.Column('allow_forking', sa.Boolean(), nullable=True),
    sa.Column('is_template', sa.Boolean(), nullable=True),
    sa.Column('web_commit_signoff_required', sa.Boolean(), nullable=True),
    sa.Column('topics', sa.String(), nullable=True),
    sa.Column('visibility', sa.String(), nullable=True),
    sa.Column('forks', sa.Integer(), nullable=True),
    sa.Column('open_issues', sa.Integer(), nullable=True),
    sa.Column('watchers', sa.Integer(), nullable=True),
    sa.Column('default_branch', sa.String(), nullable=True),
    sa.Column('permissions', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('core_github_popular_paths',
    sa.Column('metrics_date', sa.Date(), nullable=False, comment='The date for each metrics snapshot.'),
    sa.Column('path', sa.String(), nullable=False, comment='One of the ten most popular Github paths on a given date.'),
    sa.Column('title', sa.String(), nullable=True),
    sa.Column('total_views', sa.Integer(), nullable=True),
    sa.Column('unique_views', sa.Integer(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('metrics_date', 'path')
    )
    op.create_table('core_github_popular_referrers',
    sa.Column('metrics_date', sa.Date(), nullable=False, comment='The date for each metrics snapshot.'),
    sa.Column('referrer', sa.String(), nullable=False, comment='The unique referrer.'),
    sa.Column('total_referrals', sa.Integer(), nullable=True),
    sa.Column('unique_referrals', sa.Integer(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('metrics_date', 'referrer')
    )
    op.create_table('core_github_stargazers',
    sa.Column('id', sa.Integer(), nullable=False, comment='The unique identifier for each stargazer.'),
    sa.Column('starred_at', sa.DateTime(), nullable=True),
    sa.Column('login', sa.String(), nullable=True),
    sa.Column('node_id', sa.String(), nullable=True),
    sa.Column('url', sa.String(), nullable=True),
    sa.Column('html_url', sa.String(), nullable=True),
    sa.Column('followers_url', sa.String(), nullable=True),
    sa.Column('following_url', sa.String(), nullable=True),
    sa.Column('gists_url', sa.String(), nullable=True),
    sa.Column('starred_url', sa.String(), nullable=True),
    sa.Column('subscriptions_url', sa.String(), nullable=True),
    sa.Column('organizations_url', sa.String(), nullable=True),
    sa.Column('repos_url', sa.String(), nullable=True),
    sa.Column('events_url', sa.String(), nullable=True),
    sa.Column('received_events_url', sa.String(), nullable=True),
    sa.Column('type', sa.String(), nullable=True),
    sa.Column('site_admin', sa.Boolean(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('core_github_views',
    sa.Column('metrics_date', sa.Date(), nullable=False, comment='The date for each metrics snapshot.'),
    sa.Column('total_views', sa.Integer(), nullable=True),
    sa.Column('unique_views', sa.Integer(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('metrics_date')
    )
    op.create_table('core_kaggle_logs',
    sa.Column('metrics_date', sa.DateTime(), nullable=False, comment='The unique date for each metrics snapshot.'),
    sa.Column('total_views', sa.Integer(), nullable=True),
    sa.Column('total_downloads', sa.Integer(), nullable=True),
    sa.Column('total_votes', sa.Integer(), nullable=True),
    sa.Column('usability_rating', sa.Float(), nullable=True),
    sa.Column('dataset_name', sa.String(), nullable=True),
    sa.Column('owner', sa.String(), nullable=True),
    sa.Column('title', sa.String(), nullable=True),
    sa.Column('subtitle', sa.String(), nullable=True),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('keywords', sa.String(), nullable=True),
    sa.Column('dataset_id', sa.String(), nullable=True),
    sa.Column('is_private', sa.String(), nullable=True),
    sa.Column('licenses', sa.String(), nullable=True),
    sa.Column('collaborators', sa.String(), nullable=True),
    sa.Column('data', sa.String(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('metrics_date')
    )
    op.create_table('core_s3_logs',
    sa.Column('id', sa.String(), nullable=False, comment='A unique ID for each log.'),
    sa.Column('time', sa.DateTime(), nullable=True),
    sa.Column('request_uri', sa.String(), nullable=True),
    sa.Column('operation', sa.String(), nullable=True),
    sa.Column('bucket', sa.String(), nullable=True),
    sa.Column('bucket_owner', sa.String(), nullable=True),
    sa.Column('requester', sa.String(), nullable=True),
    sa.Column('http_status', sa.Integer(), nullable=True),
    sa.Column('megabytes_sent', sa.Float(), nullable=True),
    sa.Column('remote_ip', sa.String(), nullable=True),
    sa.Column('remote_ip_city', sa.String(), nullable=True),
    sa.Column('remote_ip_loc', sa.String(), nullable=True),
    sa.Column('remote_ip_org', sa.String(), nullable=True),
    sa.Column('remote_ip_hostname', sa.String(), nullable=True),
    sa.Column('remote_ip_country_name', sa.String(), nullable=True),
    sa.Column('remote_ip_asn', sa.String(), nullable=True),
    sa.Column('remote_ip_bogon', sa.Boolean(), nullable=True),
    sa.Column('remote_ip_country', sa.String(), nullable=True),
    sa.Column('remote_ip_timezone', sa.String(), nullable=True),
    sa.Column('remote_ip_latitude', sa.Float(), nullable=True),
    sa.Column('remote_ip_longitude', sa.Float(), nullable=True),
    sa.Column('remote_ip_postal', sa.String(), nullable=True),
    sa.Column('remote_ip_region', sa.String(), nullable=True),
    sa.Column('remote_ip_full_location', sa.String(), nullable=True),
    sa.Column('access_point_arn', sa.String(), nullable=True),
    sa.Column('acl_required', sa.String(), nullable=True),
    sa.Column('authentication_type', sa.String(), nullable=True),
    sa.Column('cipher_suite', sa.String(), nullable=True),
    sa.Column('error_code', sa.String(), nullable=True),
    sa.Column('host_header', sa.String(), nullable=True),
    sa.Column('host_id', sa.String(), nullable=True),
    sa.Column('key', sa.String(), nullable=True),
    sa.Column('object_size', sa.Float(), nullable=True),
    sa.Column('request_id', sa.String(), nullable=True),
    sa.Column('referer', sa.String(), nullable=True),
    sa.Column('signature_version', sa.String(), nullable=True),
    sa.Column('tls_version', sa.String(), nullable=True),
    sa.Column('total_time', sa.BigInteger(), nullable=True),
    sa.Column('turn_around_time', sa.Float(), nullable=True),
    sa.Column('user_agent', sa.String(), nullable=True),
    sa.Column('version_id', sa.String(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('datasette_request_logs',
    sa.Column('insert_id', sa.String(), nullable=False, comment='A unique ID for each log.'),
    sa.Column('log_name', sa.String(), nullable=True),
    sa.Column('resource', sa.String(), nullable=True),
    sa.Column('text_payload', sa.String(), nullable=True),
    sa.Column('timestamp', sa.DateTime(), nullable=True),
    sa.Column('receive_timestamp', sa.DateTime(), nullable=True),
    sa.Column('severity', sa.String(), nullable=True),
    sa.Column('http_request', sa.String(), nullable=True),
    sa.Column('labels', sa.String(), nullable=True),
    sa.Column('operation', sa.String(), nullable=True),
    sa.Column('trace', sa.String(), nullable=True),
    sa.Column('span_id', sa.String(), nullable=True),
    sa.Column('trace_sampled', sa.Boolean(), nullable=True),
    sa.Column('source_location', sa.String(), nullable=True),
    sa.Column('cache_hit', sa.String(), nullable=True),
    sa.Column('cache_lookup', sa.String(), nullable=True),
    sa.Column('request_url', sa.String(), nullable=True),
    sa.Column('protocol', sa.String(), nullable=True),
    sa.Column('cache_fill_bytes', sa.String(), nullable=True),
    sa.Column('response_size', sa.Float(), nullable=True),
    sa.Column('server_ip', sa.String(), nullable=True),
    sa.Column('cache_validated_with_origin_server', sa.String(), nullable=True),
    sa.Column('request_method', sa.String(), nullable=True),
    sa.Column('request_size', sa.Integer(), nullable=True),
    sa.Column('user_agent', sa.String(), nullable=True),
    sa.Column('status', sa.Integer(), nullable=True),
    sa.Column('referer', sa.String(), nullable=True),
    sa.Column('latency', sa.Float(), nullable=True),
    sa.Column('remote_ip', sa.String(), nullable=True),
    sa.Column('request_url_path', sa.String(), nullable=True),
    sa.Column('request_url_query', sa.String(), nullable=True),
    sa.Column('request_url_scheme', sa.String(), nullable=True),
    sa.Column('request_url_netloc', sa.String(), nullable=True),
    sa.Column('remote_ip_city', sa.String(), nullable=True),
    sa.Column('remote_ip_loc', sa.String(), nullable=True),
    sa.Column('remote_ip_org', sa.String(), nullable=True),
    sa.Column('remote_ip_hostname', sa.String(), nullable=True),
    sa.Column('remote_ip_country_name', sa.String(), nullable=True),
    sa.Column('remote_ip_asn', sa.String(), nullable=True),
    sa.Column('remote_ip_country', sa.String(), nullable=True),
    sa.Column('remote_ip_timezone', sa.String(), nullable=True),
    sa.Column('remote_ip_latitude', sa.Float(), nullable=True),
    sa.Column('remote_ip_longitude', sa.Float(), nullable=True),
    sa.Column('remote_ip_postal', sa.String(), nullable=True),
    sa.Column('remote_ip_region', sa.String(), nullable=True),
    sa.Column('remote_ip_full_location', sa.String(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('insert_id')
    )
    op.create_table('intake_logs',
    sa.Column('insert_id', sa.String(), nullable=False, comment='A unique ID for each log.'),
    sa.Column('timestamp', sa.DateTime(), nullable=True),
    sa.Column('remote_ip', sa.String(), nullable=True),
    sa.Column('request_method', sa.String(), nullable=True),
    sa.Column('request_uri', sa.String(), nullable=True),
    sa.Column('response_status', sa.Integer(), nullable=True),
    sa.Column('request_bytes', sa.BigInteger(), nullable=True),
    sa.Column('response_bytes', sa.BigInteger(), nullable=True),
    sa.Column('response_time_taken', sa.BigInteger(), nullable=True),
    sa.Column('request_host', sa.String(), nullable=True),
    sa.Column('request_referer', sa.String(), nullable=True),
    sa.Column('request_user_agent', sa.String(), nullable=True),
    sa.Column('request_operation', sa.String(), nullable=True),
    sa.Column('request_bucket', sa.String(), nullable=True),
    sa.Column('request_object', sa.String(), nullable=True),
    sa.Column('tag', sa.String(), nullable=True),
    sa.Column('object_path', sa.String(), nullable=True),
    sa.Column('remote_ip_type', sa.String(), nullable=True),
    sa.Column('remote_ip_city', sa.String(), nullable=True),
    sa.Column('remote_ip_loc', sa.String(), nullable=True),
    sa.Column('remote_ip_org', sa.String(), nullable=True),
    sa.Column('remote_ip_hostname', sa.String(), nullable=True),
    sa.Column('remote_ip_country_name', sa.String(), nullable=True),
    sa.Column('remote_ip_asn', sa.String(), nullable=True),
    sa.Column('remote_ip_country', sa.String(), nullable=True),
    sa.Column('remote_ip_timezone', sa.String(), nullable=True),
    sa.Column('remote_ip_latitude', sa.Float(), nullable=True),
    sa.Column('remote_ip_longitude', sa.Float(), nullable=True),
    sa.Column('remote_ip_postal', sa.String(), nullable=True),
    sa.Column('remote_ip_region', sa.String(), nullable=True),
    sa.Column('remote_ip_full_location', sa.String(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('insert_id')
    )
    op.create_table('out_s3_logs',
    sa.Column('id', sa.String(), nullable=False, comment='A unique ID for each log.'),
    sa.Column('time', sa.DateTime(), nullable=True),
    sa.Column('table', sa.String(), nullable=True),
    sa.Column('version', sa.String(), nullable=True),
    sa.Column('remote_ip', sa.String(), nullable=True),
    sa.Column('remote_ip_city', sa.String(), nullable=True),
    sa.Column('remote_ip_loc', sa.String(), nullable=True),
    sa.Column('remote_ip_org', sa.String(), nullable=True),
    sa.Column('remote_ip_hostname', sa.String(), nullable=True),
    sa.Column('remote_ip_country_name', sa.String(), nullable=True),
    sa.Column('remote_ip_asn', sa.String(), nullable=True),
    sa.Column('remote_ip_bogon', sa.Boolean(), nullable=True),
    sa.Column('remote_ip_country', sa.String(), nullable=True),
    sa.Column('remote_ip_timezone', sa.String(), nullable=True),
    sa.Column('remote_ip_latitude', sa.Float(), nullable=True),
    sa.Column('remote_ip_longitude', sa.Float(), nullable=True),
    sa.Column('remote_ip_postal', sa.String(), nullable=True),
    sa.Column('remote_ip_region', sa.String(), nullable=True),
    sa.Column('remote_ip_full_location', sa.String(), nullable=True),
    sa.Column('access_point_arn', sa.String(), nullable=True),
    sa.Column('acl_required', sa.String(), nullable=True),
    sa.Column('authentication_type', sa.String(), nullable=True),
    sa.Column('megabytes_sent', sa.Float(), nullable=True),
    sa.Column('cipher_suite', sa.String(), nullable=True),
    sa.Column('error_code', sa.String(), nullable=True),
    sa.Column('host_header', sa.String(), nullable=True),
    sa.Column('host_id', sa.String(), nullable=True),
    sa.Column('http_status', sa.Integer(), nullable=True),
    sa.Column('key', sa.String(), nullable=True),
    sa.Column('object_size', sa.Float(), nullable=True),
    sa.Column('referer', sa.String(), nullable=True),
    sa.Column('request_id', sa.String(), nullable=True),
    sa.Column('request_uri', sa.String(), nullable=True),
    sa.Column('signature_version', sa.String(), nullable=True),
    sa.Column('tls_version', sa.String(), nullable=True),
    sa.Column('total_time', sa.BigInteger(), nullable=True),
    sa.Column('turn_around_time', sa.Float(), nullable=True),
    sa.Column('user_agent', sa.String(), nullable=True),
    sa.Column('version_id', sa.String(), nullable=True),
    sa.Column('partition_key', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('out_s3_logs')
    op.drop_table('intake_logs')
    op.drop_table('datasette_request_logs')
    op.drop_table('core_s3_logs')
    op.drop_table('core_kaggle_logs')
    op.drop_table('core_github_views')
    op.drop_table('core_github_stargazers')
    op.drop_table('core_github_popular_referrers')
    op.drop_table('core_github_popular_paths')
    op.drop_table('core_github_forks')
    op.drop_table('core_github_clones')
    # ### end Alembic commands ###
