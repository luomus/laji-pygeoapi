from src.app import auth, db
from src.models import RequestLog

def create_log_entry(request, response):
    current_user = auth.current_user()

    log_entry = RequestLog(
        status_code=response.status_code,
        api_key_id=current_user.id if current_user else None,
        path=request.path,
        query_string=request.query_string.decode('utf-8'),
        ip_address=_get_request_ip_address(request)
    )
    db.session.add(log_entry)
    db.session.commit()


def _get_request_ip_address(request):
    if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
        return request.environ['REMOTE_ADDR']
    else:
        # behind a proxy
        return request.environ['HTTP_X_FORWARDED_FOR'].split(',')[0]
