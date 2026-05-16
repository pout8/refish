# -*- coding: utf-8 -*-
"""
通过 Redfish API 异步获取华为服务器硬件 SN 信息。

主要适配目标:
- 华为 Atlas 800I A2 / A3
- 华为 TaiShan 200 / 鲲鹏服务器
- 其他 iBMC Redfish 风格相近的华为服务器

功能:
- 自动发现 Systems / Chassis / Storage / Drives / PCIeDevices
- 自动处理 Redfish 集合分页
- 自动提取标准字段和 Huawei OEM 字段里的 SN
- 支持 CPU、NPU、内存、硬盘、PCIe 卡、RAID 卡
- 支持命令行选择组件
- 支持 config.json 读取账号密码
"""

import argparse
import asyncio
import json
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp


CONFIG_FILE = "config.json"


# ----------------------------
# 工具函数
# ----------------------------

def safe_get(data: Dict[str, Any], path: List[str], default: Any = "N/A") -> Any:
    """安全读取嵌套字段。"""
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def pick_first(*values: Any, default: str = "N/A") -> Any:
    """返回第一个非空、非 N/A 的值。"""
    for v in values:
        if v not in (None, "", "N/A", "Unknown"):
            return v
    return default


def extract_sn(data: Dict[str, Any]) -> str:
    """
    尽量从标准字段和华为 OEM 字段中提取序列号。

    华为不同型号/固件可能出现:
    - SerialNumber
    - Oem.Huawei.SerialNumber
    - Oem.Huawei.DeviceSerialNumber
    - Oem.Huawei.PartNumber / BoardSerialNumber 等
    """
    candidates = [
        data.get("SerialNumber"),
        data.get("Serial"),
        data.get("SN"),
        safe_get(data, ["Oem", "Huawei", "SerialNumber"], None),
        safe_get(data, ["Oem", "Huawei", "DeviceSerialNumber"], None),
        safe_get(data, ["Oem", "Huawei", "BoardSerialNumber"], None),
        safe_get(data, ["Oem", "Huawei", "CardSerialNumber"], None),
        safe_get(data, ["Oem", "Huawei", "ProductSerialNumber"], None),
    ]
    return pick_first(*candidates)


def extract_model(data: Dict[str, Any]) -> str:
    """提取型号/产品名。"""
    candidates = [
        data.get("Model"),
        data.get("ProductName"),
        data.get("PartNumber"),
        data.get("Description"),
        safe_get(data, ["Oem", "Huawei", "Model"], None),
        safe_get(data, ["Oem", "Huawei", "ProductName"], None),
        safe_get(data, ["Oem", "Huawei", "PartNumber"], None),
    ]
    return pick_first(*candidates)


def bytes_to_gib(value: Optional[int]) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{round(value / (1024 ** 3))} GB"
    except Exception:
        return "N/A"


def mib_to_gib(value: Optional[int]) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{round(value / 1024)} GB"
    except Exception:
        return "N/A"


def normalize_path(path: str) -> str:
    """Redfish 路径统一处理。"""
    if not path:
        return path
    if path.startswith("http://") or path.startswith("https://"):
        # 后面由 client.get 处理完整 URL
        return path
    if not path.startswith("/"):
        return "/" + path
    return path


# ----------------------------
# Redfish Client
# ----------------------------

class RedfishClient:
    """异步 Redfish 客户端。"""

    def __init__(
        self,
        target_ip: str,
        auth: aiohttp.BasicAuth,
        session: aiohttp.ClientSession,
        debug: bool = False,
    ):
        self.target_ip = target_ip
        self._auth = auth
        self._session = session
        self.base_url = f"https://{self.target_ip}"
        self.debug = debug
        self._cache: Dict[str, Optional[Dict[str, Any]]] = {}

    async def get(self, path: str) -> Optional[Dict[str, Any]]:
        """GET Redfish JSON。带简单缓存，避免重复请求。"""
        path = normalize_path(path)

        if path.startswith("http://") or path.startswith("https://"):
            url = path
            cache_key = path
        else:
            url = f"{self.base_url}{path}"
            cache_key = path

        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            async with self._session.get(
                url,
                auth=self._auth,
                ssl=False,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status == 404:
                    self._cache[cache_key] = None
                    return None

                if response.status == 401:
                    print(
                        f"错误: {self.target_ip} 认证失败 401，请检查用户名/密码。",
                        file=sys.stderr,
                    )
                    self._cache[cache_key] = None
                    return None

                response.raise_for_status()
                data = await response.json(content_type=None)
                self._cache[cache_key] = data
                return data

        except aiohttp.ClientConnectorError:
            print(f"错误: 无法连接到 {self.target_ip}，请检查网络/IP。", file=sys.stderr)
        except asyncio.TimeoutError:
            print(f"错误: 请求超时: {url}", file=sys.stderr)
        except aiohttp.ClientResponseError as e:
            if self.debug:
                print(f"调试: 请求失败 {url}, HTTP {e.status}", file=sys.stderr)
        except json.JSONDecodeError:
            print(f"错误: JSON 解析失败: {url}", file=sys.stderr)
        except Exception as e:
            print(f"错误: 请求异常 {url}: {e}", file=sys.stderr)

        self._cache[cache_key] = None
        return None

    async def get_collection_members(self, collection_path: str) -> List[str]:
        """
        读取 Redfish 集合成员，自动处理 Members@odata.nextLink。
        返回成员 @odata.id 列表。
        """
        members: List[str] = []
        next_link: Optional[str] = normalize_path(collection_path)

        while next_link:
            data = await self.get(next_link)
            if not data:
                break

            for member in data.get("Members", []):
                if isinstance(member, dict) and member.get("@odata.id"):
                    members.append(member["@odata.id"])

            next_link = data.get("Members@odata.nextLink")

        # 去重并保持顺序
        seen: Set[str] = set()
        result: List[str] = []
        for p in members:
            if p not in seen:
                seen.add(p)
                result.append(p)
        return result

    async def discover_systems(self) -> List[str]:
        paths = await self.get_collection_members("/redfish/v1/Systems")
        if not paths:
            # 兜底兼容老脚本路径
            for p in ["/redfish/v1/Systems/1", "/redfish/v1/Systems/1/"]:
                if await self.get(p):
                    paths.append(p)
                    break
        return paths

    async def discover_chassis(self) -> List[str]:
        paths = await self.get_collection_members("/redfish/v1/Chassis")
        if not paths:
            for p in ["/redfish/v1/Chassis/1", "/redfish/v1/Chassis/1/"]:
                if await self.get(p):
                    paths.append(p)
                    break
        return paths

    async def discover_managers(self) -> List[str]:
        return await self.get_collection_members("/redfish/v1/Managers")


# ----------------------------
# 信息采集函数
# ----------------------------

async def get_system_info(client: RedfishClient) -> List[str]:
    output = ["\n--- [系统信息] ---"]

    systems = await client.discover_systems()
    if not systems:
        output.append("  无法发现 Systems 资源。")
        return output

    for system_path in systems:
        data = await client.get(system_path)
        if not data:
            continue

        output.append(f"  资源路径: {system_path}")
        output.append(f"  名称: {data.get('Name', 'N/A')}")
        output.append(f"  制造商: {data.get('Manufacturer', 'N/A')}")
        output.append(f"  型号: {data.get('Model', 'N/A')}")
        output.append(f"  系统 SN: {extract_sn(data)}")
        output.append(f"  BIOS 版本: {data.get('BiosVersion', 'N/A')}")
        output.append(f"  电源状态: {data.get('PowerState', 'N/A')}")
        output.append(f"  健康状态: {safe_get(data, ['Status', 'Health'])}")

    return output


async def get_cpu_npu_info(client: RedfishClient) -> List[str]:
    output = ["\n--- [处理器信息: CPU / NPU / Accelerator] ---"]

    systems = await client.discover_systems()
    if not systems:
        output.append("  无法发现 Systems 资源。")
        return output

    cpu_count = 0
    npu_count = 0
    other_count = 0

    for system_path in systems:
        system_data = await client.get(system_path)
        if not system_data:
            continue

        processors_link = safe_get(system_data, ["Processors", "@odata.id"], None)
        if not processors_link:
            # 兜底拼接
            processors_link = system_path.rstrip("/") + "/Processors"

        processor_paths = await client.get_collection_members(processors_link)
        if not processor_paths:
            output.append(f"  {system_path}: 无法获取处理器列表。")
            continue

        tasks = [client.get(p) for p in processor_paths]
        results = await asyncio.gather(*tasks)

        for path, proc in zip(processor_paths, results):
            if not proc:
                continue

            name = proc.get("Name", "N/A")
            model = extract_model(proc)
            sn = extract_sn(proc)
            ptype = proc.get("ProcessorType", "")
            lower_text = f"{name} {model} {ptype} {path}".lower()

            if "npu" in lower_text or "accelerator" in lower_text or "ascend" in lower_text:
                npu_count += 1
                dev_type = "NPU/加速器"
            elif "cpu" in lower_text or ptype.lower() == "cpu":
                cpu_count += 1
                dev_type = "CPU"
            else:
                other_count += 1
                dev_type = "处理器/未知"

            output.append(
                f"  - [{dev_type}] 路径: {path}, 名称: {name}, 型号: {model}, SN: {sn}"
            )

    output.insert(1, f"  CPU 数量: {cpu_count}")
    output.insert(2, f"  NPU/加速器数量: {npu_count}")
    if other_count:
        output.insert(3, f"  其他处理器资源数量: {other_count}")

    return output


async def get_memory_info(client: RedfishClient) -> List[str]:
    output = ["\n--- [内存信息] ---"]

    systems = await client.discover_systems()
    if not systems:
        output.append("  无法发现 Systems 资源。")
        return output

    mem_paths: List[str] = []

    for system_path in systems:
        system_data = await client.get(system_path)
        if not system_data:
            continue

        memory_link = safe_get(system_data, ["Memory", "@odata.id"], None)
        if not memory_link:
            memory_link = system_path.rstrip("/") + "/Memory"

        mem_paths.extend(await client.get_collection_members(memory_link))

    # 去重
    mem_paths = sorted(set(mem_paths))

    if not mem_paths:
        output.append("  无法获取内存列表。")
        return output

    output.append(f"  内存条数量: {len(mem_paths)}")

    tasks = [client.get(p) for p in mem_paths]
    results = await asyncio.gather(*tasks)

    for path, mem in zip(mem_paths, results):
        if not mem:
            continue

        name = mem.get("Name", "N/A")
        size_gb = mib_to_gib(mem.get("CapacityMiB"))
        sn = extract_sn(mem)
        manufacturer = mem.get("Manufacturer", "N/A")
        part_number = mem.get("PartNumber", "N/A")
        speed = pick_first(mem.get("OperatingSpeedMhz"), mem.get("AllowedSpeedsMHz"), default="N/A")

        output.append(
            f"  - 路径: {path}, 名称: {name}, 容量: {size_gb}, "
            f"厂商: {manufacturer}, PN: {part_number}, 频率: {speed}, SN: {sn}"
        )

    return output


async def discover_drive_paths(client: RedfishClient) -> List[str]:
    """
    发现硬盘路径。

    策略:
    1. Chassis/*/Drives
    2. Systems/*/Storage 或 Storages
    3. StorageControllers 中关联的 Drives
    """
    drive_paths: Set[str] = set()

    # 策略 1: 从 Chassis Drives 获取物理盘
    chassis_paths = await client.discover_chassis()
    for chassis_path in chassis_paths:
        chassis = await client.get(chassis_path)
        if not chassis:
            continue

        drives_link = safe_get(chassis, ["Drives", "@odata.id"], None)
        candidate_links = []

        if drives_link:
            candidate_links.append(drives_link)

        # 兜底路径
        candidate_links.extend([
            chassis_path.rstrip("/") + "/Drives",
            chassis_path.rstrip("/") + "/Drives/",
        ])

        for link in candidate_links:
            members = await client.get_collection_members(link)
            for p in members:
                drive_paths.add(p)

    # 策略 2: 从 Systems Storage/Storages 获取
    systems = await client.discover_systems()
    storage_paths: Set[str] = set()

    for system_path in systems:
        system_data = await client.get(system_path)
        if not system_data:
            continue

        for key in ("Storage", "Storages"):
            storage_link = safe_get(system_data, [key, "@odata.id"], None)
            if storage_link:
                for p in await client.get_collection_members(storage_link):
                    storage_paths.add(p)

        # 兜底兼容你的旧路径和常见新路径
        for storage_collection in [
            system_path.rstrip("/") + "/Storage",
            system_path.rstrip("/") + "/Storages",
        ]:
            for p in await client.get_collection_members(storage_collection):
                storage_paths.add(p)

    if storage_paths:
        storage_tasks = [client.get(p) for p in sorted(storage_paths)]
        storage_results = await asyncio.gather(*storage_tasks)

        for storage in storage_results:
            if not storage:
                continue

            # 标准 Drives
            for drive_member in storage.get("Drives", []):
                if isinstance(drive_member, dict) and drive_member.get("@odata.id"):
                    drive_paths.add(drive_member["@odata.id"])

            # 某些实现可能有 Drives.@odata.id
            drives_link = safe_get(storage, ["Drives", "@odata.id"], None)
            if drives_link:
                for p in await client.get_collection_members(drives_link):
                    drive_paths.add(p)

    return sorted(drive_paths)


async def get_drive_info(client: RedfishClient) -> List[str]:
    output = ["\n--- [硬盘信息] ---"]

    drive_paths = await discover_drive_paths(client)

    if not drive_paths:
        output.append("  无法获取硬盘列表。")
        return output

    output.append(f"  硬盘数量: {len(drive_paths)}")

    tasks = [client.get(p) for p in drive_paths]
    results = await asyncio.gather(*tasks)

    for path, drive in zip(drive_paths, results):
        if not drive:
            continue

        name = drive.get("Name", "N/A")
        model = extract_model(drive)
        capacity = bytes_to_gib(drive.get("CapacityBytes"))
        media_type = drive.get("MediaType", "N/A")
        protocol = pick_first(drive.get("Protocol"), drive.get("InterfaceType"), default="N/A")
        sn = extract_sn(drive)

        output.append(
            f"  - 路径: {path}, 名称: {name}, 型号: {model}, "
            f"容量: {capacity}, 类型: {media_type}, 协议: {protocol}, SN: {sn}"
        )

    return output


async def discover_pcie_paths(client: RedfishClient) -> List[str]:
    """
    发现 PCIe / RAID / 加速卡路径。

    策略:
    1. Systems/* 里的 PCIeDevices
    2. Chassis/*/PCIeDevices
    3. StorageControllers.Oem.Huawei.AssociatedCard
    """
    paths: Set[str] = set()

    systems = await client.discover_systems()
    for system_path in systems:
        system_data = await client.get(system_path)
        if not system_data:
            continue

        # 标准字段可能是数组
        for member in system_data.get("PCIeDevices", []):
            if isinstance(member, dict) and member.get("@odata.id"):
                paths.add(member["@odata.id"])

        # 也可能是链接
        pcie_link = safe_get(system_data, ["PCIeDevices", "@odata.id"], None)
        if pcie_link:
            for p in await client.get_collection_members(pcie_link):
                paths.add(p)

        # 兜底
        for link in [
            system_path.rstrip("/") + "/PCIeDevices",
            system_path.rstrip("/") + "/PCIeDevices/",
        ]:
            for p in await client.get_collection_members(link):
                paths.add(p)

    chassis_paths = await client.discover_chassis()
    for chassis_path in chassis_paths:
        chassis = await client.get(chassis_path)
        if not chassis:
            continue

        pcie_link = safe_get(chassis, ["PCIeDevices", "@odata.id"], None)
        if pcie_link:
            for p in await client.get_collection_members(pcie_link):
                paths.add(p)

        for link in [
            chassis_path.rstrip("/") + "/PCIeDevices",
            chassis_path.rstrip("/") + "/PCIeDevices/",
        ]:
            for p in await client.get_collection_members(link):
                paths.add(p)

    # 从 StorageControllers 里找 RAID 卡关联路径
    storage_paths: Set[str] = set()
    for system_path in systems:
        for storage_collection in [
            system_path.rstrip("/") + "/Storage",
            system_path.rstrip("/") + "/Storages",
        ]:
            for p in await client.get_collection_members(storage_collection):
                storage_paths.add(p)

    if storage_paths:
        storage_tasks = [client.get(p) for p in sorted(storage_paths)]
        storage_results = await asyncio.gather(*storage_tasks)

        for storage in storage_results:
            if not storage:
                continue

            for controller in storage.get("StorageControllers", []):
                if not isinstance(controller, dict):
                    continue

                card_path = safe_get(
                    controller,
                    ["Oem", "Huawei", "AssociatedCard", "@odata.id"],
                    None,
                )
                if card_path:
                    paths.add(card_path)

    return sorted(paths)


async def get_pcie_info(client: RedfishClient) -> List[str]:
    output = ["\n--- [PCIe / RAID / 加速卡信息] ---"]

    pcie_paths = await discover_pcie_paths(client)

    if not pcie_paths:
        output.append("  未发现 PCIe / RAID / 加速卡资源。")
        return output

    output.append(f"  PCIe / RAID / 加速卡数量: {len(pcie_paths)}")

    tasks = [client.get(p) for p in pcie_paths]
    results = await asyncio.gather(*tasks)

    seen_identity: Set[Tuple[str, str, str]] = set()

    for path, dev in zip(pcie_paths, results):
        if not dev:
            continue

        name = dev.get("Name", "N/A")
        model = extract_model(dev)
        sn = extract_sn(dev)
        manufacturer = dev.get("Manufacturer", "N/A")
        desc = pick_first(dev.get("Description"), dev.get("ProductName"), default="N/A")

        text = f"{name} {model} {desc}".lower()
        if "raid" in text or "sas" in text or "storage" in text:
            dev_type = "RAID/存储卡"
        elif "ascend" in text or "npu" in text or "accelerator" in text:
            dev_type = "NPU/加速卡"
        elif "nic" in text or "ethernet" in text or "network" in text or "mellanox" in text:
            dev_type = "网卡"
        elif "gpu" in text:
            dev_type = "GPU"
        else:
            dev_type = "PCIe卡"

        identity = (name, model, sn)
        if identity in seen_identity:
            continue
        seen_identity.add(identity)

        output.append(
            f"  - [{dev_type}] 路径: {path}, 名称: {name}, 型号/描述: {model}, "
            f"厂商: {manufacturer}, SN: {sn}"
        )

    return output


async def get_bmc_info(client: RedfishClient) -> List[str]:
    output = ["\n--- [BMC / 管理控制器信息] ---"]

    managers = await client.discover_managers()
    if not managers:
        output.append("  无法发现 Managers 资源。")
        return output

    for path in managers:
        data = await client.get(path)
        if not data:
            continue

        output.append(f"  资源路径: {path}")
        output.append(f"  名称: {data.get('Name', 'N/A')}")
        output.append(f"  型号: {data.get('Model', 'N/A')}")
        output.append(f"  BMC SN: {extract_sn(data)}")
        output.append(f"  固件版本: {data.get('FirmwareVersion', 'N/A')}")
        output.append(f"  UUID: {data.get('UUID', 'N/A')}")

    return output


async def get_all_sn_summary(client: RedfishClient) -> List[str]:
    """
    简洁汇总模式：适合只关心 SN 的场景。
    """
    output = ["\n--- [SN 汇总] ---"]

    sections = [
        await get_system_info(client),
        await get_cpu_npu_info(client),
        await get_memory_info(client),
        await get_drive_info(client),
        await get_pcie_info(client),
        await get_bmc_info(client),
    ]

    # 这里不再二次解析字符串，直接输出完整信息更可靠
    for section in sections:
        output.extend(section)

    return output


# ----------------------------
# 配置读取
# ----------------------------

def load_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"错误: {CONFIG_FILE} 不是有效 JSON。", file=sys.stderr)
        return {}


def get_credentials(config: Dict[str, Any], target_ip: str) -> Tuple[str, str]:
    """
    支持几种 config.json 格式。

    格式 1:
    {
      "username": "Administrator",
      "password": "xxxx"
    }

    格式 2:
    {
      "default": {
        "username": "Administrator",
        "password": "xxxx"
      }
    }

    格式 3:
    {
      "servers": {
        "192.168.1.10": {
          "username": "Administrator",
          "password": "xxxx"
        }
      }
    }
    """
    if "servers" in config and target_ip in config["servers"]:
        item = config["servers"][target_ip]
        return item.get("username", ""), item.get("password", "")

    if "default" in config:
        item = config["default"]
        return item.get("username", ""), item.get("password", "")

    return config.get("username", ""), config.get("password", "")


# ----------------------------
# 主流程
# ----------------------------

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="通过 Redfish API 获取华为服务器硬件 SN 信息。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("target_ip", help="BMC / iBMC IP 地址")
    parser.add_argument(
        "--components",
        nargs="+",
        choices=["system", "cpu", "npu", "memory", "drive", "pcie", "bmc", "all"],
        default=["all"],
        help=(
            "指定要获取的组件，可多选。\n"
            "可选: system cpu npu memory drive pcie bmc all\n"
            "默认: all"
        ),
    )
    parser.add_argument("--username", "-u", help="iBMC 用户名，优先级高于 config.json")
    parser.add_argument("--password", "-p", help="iBMC 密码，优先级高于 config.json")
    parser.add_argument("--debug", action="store_true", help="打印调试信息")
    args = parser.parse_args()

    config = load_config()
    username, password = get_credentials(config, args.target_ip)

    if args.username:
        username = args.username
    if args.password:
        password = args.password

    if not username or not password:
        print(
            "错误: 未提供用户名或密码。请使用 --username/--password，或创建 config.json。",
            file=sys.stderr,
        )
        sys.exit(1)

    auth = aiohttp.BasicAuth(username, password)

    connector = aiohttp.TCPConnector(ssl=False, limit=20)

    async with aiohttp.ClientSession(connector=connector) as session:
        client = RedfishClient(
            target_ip=args.target_ip,
            auth=auth,
            session=session,
            debug=args.debug,
        )

        components = set(args.components)
        if "all" in components:
            components = {"system", "cpu", "npu", "memory", "drive", "pcie", "bmc"}

        tasks = []

        if "system" in components:
            tasks.append(get_system_info(client))

        # cpu 和 npu 共用一个 Redfish Processors 集合
        if "cpu" in components or "npu" in components:
            tasks.append(get_cpu_npu_info(client))

        if "memory" in components:
            tasks.append(get_memory_info(client))

        if "drive" in components:
            tasks.append(get_drive_info(client))

        if "pcie" in components:
            tasks.append(get_pcie_info(client))

        if "bmc" in components:
            tasks.append(get_bmc_info(client))

        results = await asyncio.gather(*tasks)

        print(f"\n========== {args.target_ip} 硬件 SN 信息 ==========")
        for section in results:
            for line in section:
                print(line)

        print("\n信息获取完成。")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
