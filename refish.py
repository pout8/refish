# -*- coding: utf-8 -*-
"""
一个通过 Redfish API 异步获取服务器硬件信息的 Python 脚本。

功能特性:
- 异步I/O: 使用 aiohttp 和 asyncio 并发执行网络请求，极大提升效率。
- 配置分离: 从 'config.json' 文件读取服务器 IP 和凭据，保证代码整洁与安全。
- 模块化设计: 每个硬件组件信息获取函数返回格式化数据，由主函数统一有序打印。
- 命令行接口: 支持通过命令行参数指定目标 IP 和需要查询的硬件组件。
- 适应性增强: 能够从标准路径和厂商特定路径（如存储控制器关联的板卡）获取设备信息。
- 健壮的发现逻辑: 优先从最可靠的API路径获取信息，并提供后备路径以兼容不同服务器型号。
- 分页处理: 自动处理 Redfish API 的分页响应，确保获取完整的集合数据。
"""
import asyncio
import json
import argparse
import sys
import aiohttp
from typing import Dict, Any, List, Set, Optional

# --- 配置和设置 ---
CONFIG_FILE = 'config.json'  # 配置文件名


class RedfishClient:
    """为特定目标管理异步 Redfish API 请求的客户端。"""

    def __init__(self, target_ip: str, auth: aiohttp.BasicAuth, session: aiohttp.ClientSession):
        self.target_ip = target_ip
        self._auth = auth
        self._session = session
        self.BASE_URL = f"https://{self.target_ip}"

    async def get(self, path: str) -> Optional[Dict[str, Any]]:
        """对给定的 Redfish 路径执行异步 GET 请求。"""
        url = f"{self.BASE_URL}{path}"
        try:
            async with self._session.get(url, auth=self._auth, ssl=False, timeout=30) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientConnectorError:
            print(f"错误: 无法连接到 {self.target_ip}。请检查 IP 地址和网络连接。", file=sys.stderr)
        except aiohttp.ClientResponseError as e:
            if e.status == 401:
                print(f"错误: 认证失败 (401 Unauthorized)。请检查 'config.json' 中的用户名和密码是否正确。",
                      file=sys.stderr)
            else:
                # 对于404等错误，静默处理，让调用者判断返回的None
                pass
        except asyncio.TimeoutError:
            print(f"错误: 请求超时，URL: {url}", file=sys.stderr)
        except json.JSONDecodeError:
            print(f"错误: 解析 JSON 响应失败，URL: {url}", file=sys.stderr)
        except Exception as e:
            print(f"错误: 发生未知错误，URL: {url}。错误详情: {e}", file=sys.stderr)
        return None


# --- 数据获取与格式化函数 ---

def format_drive_details(drive_data: Dict[str, Any]) -> str:
    """将单个硬盘的JSON数据格式化为一行字符串。"""
    name = drive_data.get('Name', 'N/A')
    model = drive_data.get('Model', 'N/A')
    capacity_bytes = drive_data.get('CapacityBytes')
    size_gb = f"{round(capacity_bytes / (1024 ** 3))} GB" if capacity_bytes is not None else "N/A"
    sn = drive_data.get('SerialNumber', 'N/A')
    media_type = drive_data.get('MediaType', 'N/A')
    return f"  - [{name}] 型号: {model}, 容量: {size_gb}, 类型: {media_type}, SN: {sn}"


async def get_system_info(client: RedfishClient) -> List[str]:
    """获取基本的系统信息并格式化为字符串列表。"""
    output = ["\n--- [系统信息] ---"]
    data = await client.get('/redfish/v1/Systems/1/')
    if data:
        output.append(f"  制造商: {data.get('Manufacturer', 'N/A')}")
        output.append(f"  型号: {data.get('Model', 'N/A')}")
        output.append(f"  序列号 (SN): {data.get('SerialNumber', 'N/A')}")
        output.append(f"  BIOS 版本: {data.get('BiosVersion', 'N/A')}")
        power_state = data.get('PowerState', 'N/A')
        health_status = data.get('Status', {}).get('Health', 'N/A')
        output.append(f"  电源状态: {power_state}")
        output.append(f"  健康状态: {health_status}")
    else:
        output.append("  无法获取系统信息。")
    return output


async def get_cpu_info(client: RedfishClient) -> List[str]:
    """获取 CPU 和 NPU 信息并格式化为字符串列表。"""
    output = ["\n--- [处理器信息 (CPU & NPU)] ---"]
    processors_list = await client.get('/redfish/v1/Systems/1/Processors/')
    if not processors_list or 'Members' not in processors_list:
        output.append("  无法获取处理器列表。")
        return output

    cpu_paths = [m['@odata.id'] for m in processors_list['Members'] if
                 m.get('@odata.id') and 'Npu' not in m['@odata.id']]
    npu_count = sum(1 for m in processors_list['Members'] if m.get('@odata.id') and 'Npu' in m['@odata.id'])

    output.append(f"  物理 CPU 数量: {len(cpu_paths)}")
    # 修正NPU数量计算逻辑，如果需要的话可以调整
    # 例如：output.append(f"  NPU 数量: {npu_count // 2 if npu_count > 0 else 0}")
    output.append(f"  NPU 数量: {npu_count}")

    cpu_tasks = [client.get(path) for path in cpu_paths]
    results = await asyncio.gather(*cpu_tasks)

    for cpu_data in results:
        if cpu_data:
            name = cpu_data.get('Name', 'N/A')
            model = cpu_data.get('Model', 'N/A')
            sn = cpu_data.get('Oem', {}).get('Huawei', {}).get('SerialNumber', 'N/A')
            output.append(f"  - [{name}] 型号: {model}, SN: {sn}")
    return output


async def get_memory_info(client: RedfishClient) -> List[str]:
    """获取内存信息并格式化为字符串列表，自动处理分页。"""
    output = ["\n--- [内存信息] ---"]
    all_members = []
    # 初始请求路径
    next_link = '/redfish/v1/Systems/1/Memory/'
    total_count = "N/A"

    # 循环处理分页，直到没有 nextLink 为止
    while next_link:
        page_data = await client.get(next_link)
        if not page_data:
            # 如果请求失败，则跳出循环
            break

        # 从第一页响应中获取总数
        if total_count == "N/A":
            total_count = page_data.get('Members@odata.count', 0)

        # 将当前页的成员添加到总列表中
        if 'Members' in page_data:
            all_members.extend(page_data['Members'])

        # 获取下一页的链接，如果没有则为 None，循环将终止
        next_link = page_data.get('Members@odata.nextLink')

    if not all_members:
        output.append("  无法获取内存列表。")
        return output

    output.append(f"  内存条数量: {total_count}")

    # 使用收集到的所有成员路径并发获取详细信息
    mem_tasks = [client.get(member['@odata.id']) for member in all_members if '@odata.id' in member]
    results = await asyncio.gather(*mem_tasks)

    for mem_data in results:
        if mem_data:
            name = mem_data.get('Name', 'N/A')
            size_mib = mem_data.get('CapacityMiB')
            size_gb = f"{round(size_mib / 1024)} GB" if size_mib is not None else "N/A"
            sn = mem_data.get('SerialNumber', 'N/A')
            output.append(f"  - [{name}] 容量: {size_gb}, SN: {sn}")

    return output


async def get_drive_info(client: RedfishClient) -> List[str]:
    """获取硬盘信息，优先使用Chassis路径，失败则回退到Storage路径。"""
    output = ["\n--- [硬盘信息] ---"]
    drive_path_set: Set[str] = set()

    # --- 策略 1: 首选路径，从Chassis获取完整的物理硬盘列表 ---
    drive_list_from_chassis = await client.get('/redfish/v1/Chassis/1/Drives/')
    if drive_list_from_chassis and 'Members' in drive_list_from_chassis and drive_list_from_chassis['Members']:
        for member in drive_list_from_chassis['Members']:
            if '@odata.id' in member:
                drive_path_set.add(member['@odata.id'])

    # --- 策略 2: 后备路径，如果首选路径失败，则从Storage控制器获取 ---
    if not drive_path_set:
        storage_list = await client.get('/redfish/v1/Systems/1/Storages/')
        if storage_list and 'Members' in storage_list:
            storage_tasks = [client.get(member['@odata.id']) for member in storage_list['Members']]
            storage_results = await asyncio.gather(*storage_tasks)
            for storage_data in storage_results:
                if storage_data and 'Drives' in storage_data:
                    for drive_member in storage_data['Drives']:
                        if isinstance(drive_member, dict) and '@odata.id' in drive_member:
                            drive_path_set.add(drive_member['@odata.id'])

    if not drive_path_set:
        output.append("  无法获取硬盘列表。")
        return output

    drive_paths = sorted(list(drive_path_set))
    output.append(f"  硬盘数量: {len(drive_paths)}")

    drive_tasks = [client.get(path) for path in drive_paths]
    results = await asyncio.gather(*drive_tasks)

    for drive_data in results:
        if drive_data:
            output.append(format_drive_details(drive_data))

    return output


async def get_pcie_info(client: RedfishClient) -> List[str]:
    """获取PCIe和RAID卡信息，合并标准路径和厂商特定路径的结果。"""
    output = ["\n--- [PCIe 及板卡信息] ---"]
    device_path_set: Set[str] = set()
    unique_devices: Dict[str, Dict[str, Any]] = {}

    # 1. 从标准PCIeDevices路径获取
    system_data = await client.get('/redfish/v1/Systems/1/')
    if system_data and 'PCIeDevices' in system_data and system_data['PCIeDevices']:
        for member in system_data['PCIeDevices']:
            if '@odata.id' in member:
                device_path_set.add(member['@odata.id'])

    # 2. 从Storage控制器关联的板卡路径获取 (用于发现RAID卡等)
    storage_list = await client.get('/redfish/v1/Systems/1/Storages/')
    if storage_list and 'Members' in storage_list:
        storage_tasks = [client.get(m['@odata.id']) for m in storage_list['Members']]
        storage_results = await asyncio.gather(*storage_tasks)
        for storage_data in storage_results:
            if storage_data and 'StorageControllers' in storage_data:
                for controller in storage_data['StorageControllers']:
                    card_path = controller.get('Oem', {}).get('Huawei', {}).get('AssociatedCard', {}).get('@odata.id')
                    if card_path:
                        device_path_set.add(card_path)

    if not device_path_set:
        output.append("  未找到任何 PCIe 或板卡设备。")
        return output

    output.append(f"  PCIe 及板卡数量: {len(device_path_set)}")

    device_tasks = [client.get(path) for path in sorted(list(device_path_set))]
    results = await asyncio.gather(*device_tasks)

    for device_data in results:
        if device_data:
            name = device_data.get('Name', 'N/A')
            # 优先使用ProductName，其次是Description
            desc = device_data.get('ProductName', device_data.get('Description', 'N/A'))
            sn = device_data.get('SerialNumber', 'N/A')
            device_type = "RAID卡" if "RAID" in name or "RAID" in desc else "PCIe卡"
            output.append(f"  - [{name}] 类型: {device_type}, 描述: {desc}, SN: {sn}")

    return output


# --- 主要执行逻辑 ---

def load_config() -> Optional[Dict[str, Any]]:
    """从 config.json 文件加载服务器配置。"""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"错误: 配置文件 '{CONFIG_FILE}' 未找到。请根据说明创建该文件。", file=sys.stderr)
    except json.JSONDecodeError:
        print(f"错误: 配置文件 '{CONFIG_FILE}' 格式错误，不是有效的 JSON。", file=sys.stderr)
    return None


async def main():
    """主函数，用于解析命令行参数并协调数据获取流程。"""
    parser = argparse.ArgumentParser(
        description="通过 Redfish API 异步获取指定 IP 的服务器硬件信息。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("target_ip", help="要查询的 BMC 的 IP 地址。")
    parser.add_argument(
        "--components", nargs='+', choices=['system', 'cpu', 'memory', 'drive', 'pcie'],
        help="指定要获取的硬件信息组件 (可多选)。\n默认为全部获取。"
    )
    args = parser.parse_args()

    component_map = {
        'system': get_system_info, 'cpu': get_cpu_info, 'memory': get_memory_info,
        'drive': get_drive_info, 'pcie': get_pcie_info,
    }

    # 确定要运行的组件
    components_to_run = args.components if args.components else list(component_map.keys())

    config = load_config()

    print("\n信息获取完成。")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
