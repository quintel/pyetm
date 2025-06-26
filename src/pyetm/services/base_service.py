from functools import wraps

from pyetm.services.service_result import ServiceResult, GenericError

''' Base service decorators assists runners by catching exceptions'''
def base_service(runner):
    @wraps(runner)
    def with_returning_service_results(*args, **kwargs):
        try:
            return runner(*args, **kwargs)
        except GenericError as error:
            msg = str(error)
            try:
                code = int(msg.split()[1].rstrip(':'))
            except Exception:
                code = None
            return ServiceResult(
                success=False,
                errors=[msg],
                status_code=code
            )
        except Exception as e:
            #TODO: catch more exceptions
            return ServiceResult(success=False, errors=[str(e)])

    return with_returning_service_results
