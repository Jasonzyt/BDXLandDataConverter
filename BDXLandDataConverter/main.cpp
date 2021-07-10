#include <string>
#include <iostream>
#include <fstream>
#include <Windows.h>
#include <functional>
#include <string_view>
#include <unordered_map>
#include <leveldb\db.h>
#include <leveldb\cache.h>
#include <leveldb\c.h>
#include <leveldb\iterator.h>
#include <leveldb\filter_policy.h>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#pragma warning(disable:4996)
#undef min
#undef max

using namespace std;

#pragma region BDXLand_Code_by_Sysca11
#define to_lpos(x) ((x) ^ 0x80000000)
typedef UINT32 u32;
typedef UINT16 u16;
typedef u32 lpos_t;
typedef unsigned long long xuid_t;
template<typename T>
static inline auto S(T&& x) {
	using DT = std::decay_t<T>;
	if constexpr (!(std::is_same_v<DT, string> || std::is_same_v<DT, string_view> || std::is_same_v<DT, const char*>))
		return std::to_string(std::forward<T>(x));
	else return x;
}
using std::string_view, std::string;
class KVDBImpl {
	leveldb::DB* db;
	leveldb::ReadOptions rdopt;
	leveldb::WriteOptions wropt;
	leveldb::Options options;
	string dpath;
public:
	void __init(const char* path, bool read_cache, int cache_sz, int Bfilter_bit)
	{
		{
			rdopt = leveldb::ReadOptions();
			wropt = leveldb::WriteOptions();
			options = leveldb::Options();
			rdopt.fill_cache = read_cache;
			rdopt.verify_checksums = false;
			wropt.sync = false;
			if (cache_sz) {
				options.block_cache = leveldb::NewLRUCache(cache_sz);
			}
			options.reuse_logs = true; //WARN:EXPERIMENTAL
			if (Bfilter_bit)
				options.filter_policy = leveldb::NewBloomFilterPolicy(Bfilter_bit);
			options.create_if_missing = true;
			dpath = path;
			leveldb::Status status = leveldb::DB::Open(options, path, &db);
			if (!status.ok()) {
				printf("cannot load %s reason: %s", path, status.ToString().c_str());
				exit(1);
			}
		}
	}
	~KVDBImpl()
	{
		if (options.filter_policy)
			delete options.filter_policy;
		delete db;
	};
	KVDBImpl() {}
	KVDBImpl(KVDBImpl const&) = delete;
	KVDBImpl& operator = (KVDBImpl const&) = delete;

	bool get(string_view key, string& val);
	void put(string_view key, string_view val);
	void del(string_view key);
	void iter(std::function<bool(string_view key, string_view val)> const& fn)
	{
		leveldb::Iterator* it = db->NewIterator(rdopt);
		for (it->SeekToFirst(); it->Valid(); it->Next()) {
			auto k = it->key();
			auto v = it->value();
			if (!fn({ k.data(), k.size() }, { v.data(), v.size() }))
				break;
		}
		delete it;
	}
	void iter(std::function<bool(string_view key)> const&);
};
unique_ptr<KVDBImpl> MakeKVDB(const string& path, bool read_cache = true, int cache_sz = 0, int Bfilter_bit = 0) {
	auto db = make_unique<KVDBImpl>();
	db->__init(path.c_str(), read_cache, cache_sz, Bfilter_bit);
	return db;
}
#define CINLINE constexpr inline

template<typename T>
constexpr bool is_sval = std::is_pod_v<T> && (sizeof(T) <= 8);
template<typename T = char>
struct array_view {
	using ARGTP = std::conditional_t<is_sval<T>, T, const T&>;
	static constexpr size_t npos = std::numeric_limits<long long>::max();
	T* data;
	size_t siz;
	CINLINE size_t size() const {
		return siz;
	}
	CINLINE array_view<T> slice(size_t startidx, size_t _endidx = npos) const { //[start,end)
		auto endidx = std::min(siz, _endidx);
		return array_view<T>{data + startidx, endidx - startidx};
	}
	CINLINE array_view<T> subview(size_t startidx, size_t _count = npos) const {
		auto count = std::min(siz - startidx, _count);
		return array_view<T>{data + startidx, count};
	}
	CINLINE T const& operator [](int idx) const {
		return data[idx];
	}
	CINLINE T& operator [](int idx) {
		return data[idx];
	}
	struct iterator {
		T* now;
		size_t remain;
		CINLINE bool operator!=(const iterator& r) {
			return remain != r.remain;
		}
		CINLINE bool operator==(const iterator& r) {
			return remain == r.remain;
		}
		inline void operator++() {
			now++;
			remain--;
		}
		inline void operator--() {
			now--;
			remain++;
		}
		CINLINE T& operator*() {
			return *now;
		}
		CINLINE T& operator->() {
			return *now;
		}
	};
	CINLINE iterator begin() {
		return { data,size() };
	}
	CINLINE iterator end() const {
		return { data,0 };
	}
	bool has(ARGTP x) const {
		for (size_t i = 0; i < siz; ++i) {
			if (data[i] == x) return true;
		}
		return false;
	}
	size_t count(ARGTP x) const {
		size_t rv = 0;
		for (size_t i = 0; i < siz; ++i) if (data[i] == x) rv++;
		return rv;
	}
	size_t find(ARGTP x, size_t start = 0) const {
		for (size_t i = start; i < siz; ++i) if (data[i] == x) return i;
		return npos;
	}
	bool toBack_Pop(ARGTP x) {
		auto pos = find(x);
		if (pos != npos) {
			std::swap(data[pos], data[siz - 1]);
			siz--;
			return true;
		}
		return false;
	}
	CINLINE operator string_view() const {
		return { (const char*)data,siz * sizeof(T) };
	}
	array_view(string_view v) {
		data = (T*)v.data();
		siz = v.size() / sizeof(T);
	}
	array_view(const T* s, const T* e) {
		data = (T*)s;
		siz = std::distance(s, e);
	}
	array_view(const T* s, size_t sz) {
		data = (T*)s;
		siz = sz;
	}
	array_view(T const& x) {
		siz = 1;
		data = (T*)&x;
	}
};
enum LandPerm : u16 {
	NOTHING = 0,   //no perm specified , must be owner to operate
	PERM_INTERWITHACTOR = 1,
	PERM_USE = 2,  //null hand place
	PERM_ATK = 4,  //attack entity
	PERM_BUILD = 8, //nonnull hand place
	PERM_DESTROY = 16,
	PERM_ITEMFRAME = 32,
	PERM_FULL = 63
};
struct FastLand {
	lpos_t x, z, dx, dz; //4*int
	u32 lid; //5*int
	u16 refcount;
	LandPerm perm_group; //6*int
	u16 dim;
	LandPerm perm_others; //7*int
	u32 owner_sz; //8*int
	xuid_t owner[0];
	inline int getOPerm(xuid_t x) const 
	{
		u32 owner_sz2 = owner_sz >> 3;
		for (u32 i = 0; i < owner_sz2; ++i) {
			if (owner[i] == x) return 1 + (i == 0);
		}
		return 0;
	}
	bool hasPerm(xuid_t x, LandPerm PERM) const 
	{
		switch (getOPerm(x))
		{
		case 0://others
			return (PERM & perm_others) != 0;
		case 1://group
			return (PERM & perm_group) != 0;
		case 2://owner
			return 1;
		}
		return 0; //never runs here
	}
};
#pragma endregion

using namespace rapidjson;

bool HasPermission(LandPerm perm, LandPerm val)
{
	return (perm & val) != 0;
}

int main(int argc, char** argv)
{
	string input_path, output_path;
	printf("BDXLand数据库转换器 v1.0.0 Author: Jasonzyt(Supported by Shad0w23333)\n");
	printf("目前只支持BDXLand转换PFEssentials!\n\n");
	if (argc == 3)
	{
		input_path = argv[1];
		output_path = argv[2];
	}
	else
	{
		printf("请输入BDXLand数据所在目录: ");
		cin >> input_path;
		printf("请输入输出JSON文件路径(不存在自动创建): ");
		cin >> output_path;
	}
	LARGE_INTEGER freq;
	LARGE_INTEGER begin;
	LARGE_INTEGER end;
	double timec;
	QueryPerformanceFrequency(&freq);
	QueryPerformanceCounter(&begin);
	unique_ptr<KVDBImpl> db = MakeKVDB(input_path);
	Document d(kObjectType);
	auto alloc = d.GetAllocator();
	Value val(kArrayType);
	int i = 0;
	int ii = 0;
	printf("=========== Iterator begin ===========\n");
	db->iter([&](string_view k, string_view v) -> bool
		{
			if (k.size() == 4)
			{
				string owners = "";
				Value data(kObjectType),
					shared_player(kArrayType),
					public_perm(kObjectType),
					default_perm(kObjectType);

				FastLand* fl = (FastLand*)v.data();
				// Land owners
				u32 owner_sz = fl->owner_sz >> 3;
				for (u32 j = 1; j < owner_sz; ++j)
				{
					Value sp_member(kObjectType);
					string owner = to_string(fl->owner[j]);
					sp_member.AddMember("PlayerXuid", Value().SetString(owner.c_str(), alloc), alloc);
					shared_player.PushBack(sp_member, alloc);
					owners += owner + ", ";
				}
				if (!owners.empty()) owners.pop_back();
				if (!owners.empty()) owners.pop_back();
				// Land position
				int p_x = to_lpos(fl->x);
				int p_dx = to_lpos(fl->dx);
				int p_z = to_lpos(fl->z);
				int p_dz = to_lpos(fl->dz);
				// Land public permission
				if (fl->perm_others != 0)
				{
					if (HasPermission(PERM_USE, fl->perm_others))
						public_perm.AddMember("UseItem", true, alloc);
					if (HasPermission(PERM_ATK, fl->perm_others))
					{
						public_perm.AddMember("AttackMob", true, alloc);
						public_perm.AddMember("AttackPlayer", true, alloc);
					}
					if (HasPermission(PERM_BUILD, fl->perm_others))
						public_perm.AddMember("PlaceBlock", true, alloc);
					if (HasPermission(PERM_DESTROY, fl->perm_others))
						public_perm.AddMember("DestroyBlock", true, alloc);
				}
				// Land member default permission
				if (fl->perm_group != 63)
				{
					if (!HasPermission(PERM_USE, fl->perm_group))
						default_perm.AddMember("UseItem", false, alloc);
					if (!HasPermission(PERM_ATK, fl->perm_group))
					{
						default_perm.AddMember("AttackMob", false, alloc);
						default_perm.AddMember("AttackPlayer", false, alloc);
					}
					if (!HasPermission(PERM_BUILD, fl->perm_group))
						default_perm.AddMember("PlaceBlock", false, alloc);
					if (!HasPermission(PERM_DESTROY, fl->perm_group))
						default_perm.AddMember("DestroyBlock", false, alloc);
				}
				
				if (owner_sz > 0) data.AddMember("Owner", Value().SetString(to_string(fl->owner[0]).c_str(), alloc), alloc);
				data.AddMember("Type", Value().SetString("2D", alloc), alloc);
				data.AddMember("PlayerShared", shared_player, alloc);
				data.AddMember("Dimension", fl->dim, alloc);
				data.AddMember("X1", p_x, alloc);
				data.AddMember("X2", p_dx, alloc);
				data.AddMember("Z1", p_z, alloc);
				data.AddMember("Z2", p_dz, alloc);
				data.AddMember("Y1", 64, alloc);
				data.AddMember("Y2", 64, alloc);
				data.AddMember("Public", public_perm, alloc);
				data.AddMember("DefaultShared", default_perm, alloc);
				printf("%d ---------------------------------\n", i);
				printf("- Land ID %hu Perm %hu : %hu\n", fl->lid, fl->perm_group, fl->perm_others); 
				printf("- Land POS (%d %d) -> (%d %d) Dim %hu\n", p_x, p_z, p_dx, p_dz, fl->dim); 
				printf("- Land SuperOwner: %llu | Owners(%s)\n", fl->owner[0], owners.c_str());
				val.PushBack(data, alloc);
				ii++;
			}
			i++;
		});
	d.AddMember("Land", val, alloc);
	StringBuffer buf;
	PrettyWriter<StringBuffer> writer(buf);
	d.Accept(writer);
	FILE* fp = fopen(output_path.c_str(), "w+");
	fseek(fp, 0, 0);
	fwrite(buf.GetString(), buf.GetSize(), 1, fp);

	QueryPerformanceCounter(&end);
	timec = (double)(end.QuadPart - begin.QuadPart) / (double)freq.QuadPart; // 获取计时结果
	printf("TOTAL KEYS: %d\n", i);
	printf("TOTAL RECORDS: %d\n", ii);
	printf("USED TIME: %.3f%s\n", timec, "s");

	system("pause");
	return 0;
}
