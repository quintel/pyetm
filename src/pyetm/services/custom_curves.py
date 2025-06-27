import io

from pyetm.services.base_service import base_service
from pyetm.services.service_result import ServiceResult

@base_service
def download_curve(client, scenario, curve_name: str) -> ServiceResult:
    response = client.session.get(f"/scenarios/{scenario.id}/custom_curves/{curve_name}.csv")

    if response.ok:
        return ServiceResult(
            success=True,
            data=io.StringIO(response.content.decode('utf-8')),
            status_code=response.status_code
        )

    return ServiceResult(
        success=False,
        errors=[f"{response.status_code}: {response.text}"],
        status_code=response.status_code
    )

@base_service
def fetch_all_curve_data(client, scenario) -> ServiceResult:
    response = client.session.get(f"/scenarios/{scenario.id}/custom_curves")

    if response.ok:
        return ServiceResult(
            success=True,
            data=response.json(),
            status_code=response.status_code
        )

    return ServiceResult(
        success=False,
        errors=[f"{response.status_code}: {response.text}"],
        status_code=response.status_code
    )


