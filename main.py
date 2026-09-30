from moduls import *

def download_script(url):
    """
    URL에서 스크립트를 다운로드합니다.
    - URL: 다운로드할 스크립트의 주소.
    - 반환: 스크립트 내용 (문자열).
    """
    headers = {'Cache-Control': 'no-cache'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

def load_module_from_string(module_name, module_content):
    """
    문자열 형태의 코드를 동적으로 모듈로 로드합니다.
    - module_name: 모듈 이름.
    - module_content: 모듈 코드 (문자열).
    """
    module = types.ModuleType(module_name)
    exec(module_content, module.__dict__)
    sys.modules[module_name] = module
    return module

def execute_script(script_content):
    exec(script_content, globals())


def download_and_load_all_scripts(scripts_json_url):
    """
    scripts.json에 명시된 모든 스크립트를 다운로드 후 모듈로 로드합니다.
    - scripts_json_url: 스크립트 목록이 담긴 JSON 파일의 URL.
    """
    scripts_content = download_script(scripts_json_url)
    scripts_data = json.loads(scripts_content)
    for script_url in scripts_data["scripts"]:
        script_name = script_url.split('/')[-1].split('.')[0]
        script_content = download_script(script_url)
        load_module_from_string(script_name, script_content)

print("Ver 2.0")
try:
    #scripts.json 파일의 URL
    scripts_json_url = "https://raw.githubusercontent.com/jang-sin/kgeo/main/scripts.json"

    # 모든 스크립트 다운로드 및 로드
    download_and_load_all_scripts(scripts_json_url)

    # 메인 스크립트 실행
    main_script_content = download_script("https://raw.githubusercontent.com/jang-sin/kgeo/main/apps.py")
    execute_script(main_script_content)

except Exception as e:
    print(f"[main.py] An error occurred: {e}")

    # 스크립트 끝에서 사용자 입력을 기다려 창이 바로 닫히지 않도록 합니다
    input("Press Enter to exit...")