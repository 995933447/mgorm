package main

import (
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/995933447/mgorm"
	"github.com/995933447/mgorm/pb"
	"github.com/995933447/runtimeutil"
	"github.com/995933447/stringhelper-go"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/pluginpb"
)

const EnvKeyEnabledFprintPb = "MGORM_ENABLED_FPRINT_PB"

const (
	OrmStructNameSuffix = "Orm"
	OrmModelNameSuffix  = "Model"
	PrimaryIdFieldName  = "ID"
	ConnName            = mgorm.ConnNameDefault
)

//go:embed template/header.tmpl
var headerTmpl string

//go:embed template/body_sec.tmpl
var bodySectionTmpl string

func init() {
	// 自定义的 protoc 插件（例如 protoc-gen-xxx）必须通过 标准输入/输出 (stdin/stdout) 与 protoc 交互
	// 避免log 输出污染了 stdout，log 会把内容写到 stdout，而 protoc 会把 stdout 当成 CodeGeneratorResponse 解析。
	log.SetOutput(os.Stderr)
}

type TemplateHeaderSlot struct {
	ImportPathBase        string
	ExistsModel           bool
	DisabledAutoExpireAt  bool
	DisabledAutoUpdatedAt bool
	DisabledAutoCreatedAt bool
	ExtraImportPaths      []string
	Cached                bool
	ShouldImportPrimitive bool
	ShouldImportTime      bool
}

type TemplateBodySlot struct {
	MessageName               string
	FirstCharLowerMessageName string

	DbName   string
	TbName   string
	ConnName string

	PrimaryIdFieldName string

	Fields []*Field

	IndexKeys       []string
	UniqIndexKeys   []string
	ExpireIndexKeys []string

	TopFieldUniqIdxInfos []*IndexInfo
	TopFieldIdxInfos     []*IndexInfo

	ExpireTtlDays int64

	HasConnIdxes bool
	HasDbIdxes   bool
	HasTbIdxes   bool

	ConnIdxTypes []string
	DbIdxTypes   []string
	TbIdxTypes   []string

	Cached bool

	DisabledAutoCreatedAt bool
	DisabledAutoUpdatedAt bool
	DisabledAutoExpireAt  bool

	OrmStructNameSuffix string
	OrmModelNameSuffix  string

	ShouldGenModel bool

	NestedMessages []*NestedMessage
}

type NestedMessage struct {
	MessageName string
	Fields      []*Field
}

type Field struct {
	Name     string
	TypeName string
	JsonTag  string
	BsonTag  string
	Tags     string
}

type IndexInfo struct {
	// 用于模板 {{range .Fields}}data.{{.Name}}{{end}}
	Fields []*IndexField
	// 用于模板 {{.NamedByJoinAnd}} 生成函数名后缀
	NamedByJoinAnd string
	// 缓存键模板，如 "_username:%v_email:%v"
	CacheKeyWithPlaceholder string
}

type IndexField struct {
	Name     string // Go 字段名
	Type     string // Go 类型
	BsonName string // 数据库字段名
}

func SprintNewModelArgs(connIdxTypes, dbIdxTypes, tbIdxTypes []string) string {
	var argsStr string
	for i, v := range connIdxTypes {
		argsStr += "connIdx"
		if i > 0 {
			argsStr += fmt.Sprintf("%d", i+1)
		}
		argsStr += fmt.Sprintf(" %s, ", v)
	}

	for i, v := range dbIdxTypes {
		argsStr += "dbIdx"
		if i > 0 {
			argsStr += fmt.Sprintf("%d", i+1)
		}
		argsStr += fmt.Sprintf(" %s, ", v)
	}

	for i, v := range tbIdxTypes {
		argsStr += "tbIdx"
		if i > 0 {
			argsStr += fmt.Sprintf("%d", i+1)
		}
		argsStr += fmt.Sprintf(" %s, ", v)
	}

	return strings.TrimSuffix(argsStr, ", ")
}

func SprintConnIdxes(connIdxTypes []string) string {
	var argsStr string
	for i := range connIdxTypes {
		argsStr += "connIdx"
		if i > 0 {
			argsStr += fmt.Sprintf("%d", i+1)
		}
		argsStr += ", "
	}
	return strings.TrimSuffix(argsStr, ", ")
}

func SprintDbIdxes(dbIdxTypes []string) string {
	var argsStr string
	for i := range dbIdxTypes {
		argsStr += "dbIdx"
		if i > 0 {
			argsStr += fmt.Sprintf("%d", i+1)
		}
		argsStr += ", "
	}
	return strings.TrimSuffix(argsStr, ", ")
}

func SprintTbIdxes(tbIdxTypes []string) string {
	var argsStr string
	for i := range tbIdxTypes {
		argsStr += "tbIdx"
		if i > 0 {
			argsStr += fmt.Sprintf("%d", i+1)
		}
		argsStr += ", "
	}
	return strings.TrimSuffix(argsStr, ", ")
}

func Camel(name string) string {
	return stringhelper.LowerFirstASCII(stringhelper.Camel(name))
}

func Sub(a, b int) int {
	return a - b
}

var funcMap = template.FuncMap{
	"SprintNewModelArgs": SprintNewModelArgs,
	"SprintConnIdxes":    SprintConnIdxes,
	"SprintDbIdxes":      SprintDbIdxes,
	"SprintTbIdxes":      SprintTbIdxes,
	"Camel":              Camel,
	"Sub":                Sub,
}

func main() {
	log.Println("======= Starting protoc-gen-mgorm =========")

	debug := flag.Bool("d", false, "是否开启debug")
	inputFile := flag.String("i", "", "调试pb")
	flag.Parse() // 解析命令行参数

	var (
		input []byte
		err   error
	)
	if *debug {
		if *inputFile == "" {
			log.Fatal("input file is required")
		}

		input, err = os.ReadFile(*inputFile)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		input, err = io.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
		}

		if os.Getenv(EnvKeyEnabledFprintPb) == "true" {
			log.Println("enable debug, store input to a file: req.pb")
			err = os.WriteFile("./req.pb", input, os.ModePerm)
			if err != nil {
				log.Fatal(runtimeutil.NewStackErr(err))
			}
			return
		}
	}

	var req pluginpb.CodeGeneratorRequest
	if err := proto.Unmarshal(input, &req); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	log.Println("Files to generate:", req.GetFileToGenerate())

	opts := protogen.Options{}
	plugin, err := opts.New(&req)
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	// 建立全局 message 映射（支持嵌套）
	messageMap := make(map[string]*protogen.Message)
	for _, pf := range plugin.Files {
		for _, msg := range pf.Messages {
			registerMessages(messageMap, msg)
		}
	}

	for _, f := range plugin.Files {
		if !f.Generate {
			log.Printf("gen-grpc-mgorm, skipped gen %s\n", string(f.Desc.Name()))
			continue
		}

		//  只生成有 Service 的 proto
		if len(f.Messages) == 0 {
			log.Printf("gen-grpc-mgorm, skipped gen %s\n", string(f.Desc.Name()))
			continue
		}

		if err := genCode(plugin, f, messageMap); err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
		}
	}

	stdout := plugin.Response()
	out, err := proto.Marshal(stdout)
	if err != nil {
		panic(err)
	}

	// 必须写到 stdout
	if _, err = os.Stdout.Write(out); err != nil {
		log.Fatal(err)
	}

	log.Printf("gorm generated successfully!\n")
}

func genCode(plugin *protogen.Plugin, f *protogen.File, messageMap map[string]*protogen.Message) error {
	var headerSlot TemplateHeaderSlot
	headerSlot.ImportPathBase = path.Base(string(f.GoImportPath))
	headerSlot.DisabledAutoExpireAt = true
	headerSlot.DisabledAutoUpdatedAt = true
	headerSlot.DisabledAutoCreatedAt = true

	var bodySections []bytes.Buffer

	for _, msg := range f.Messages {
		if !proto.HasExtension(msg.Desc.Options(), pb.E_MgormOpts) {
			continue
		}

		ext := proto.GetExtension(msg.Desc.Options(), pb.E_MgormOpts).(*pb.MgormOptions)

		var bodySlot TemplateBodySlot

		if !ext.IsPureStruct {
			headerSlot.ExistsModel = true
			bodySlot.ShouldGenModel = true
		}

		bodySlot.OrmStructNameSuffix = OrmStructNameSuffix
		bodySlot.OrmModelNameSuffix = OrmModelNameSuffix
		bodySlot.PrimaryIdFieldName = PrimaryIdFieldName
		bodySlot.ConnName = ConnName

		// parseMsgFields 会返回字段列表，赋给 bodySlot.Fields
		bodySlot.Fields = parseMsgFields(f, &headerSlot, &bodySlot, msg, messageMap)

		bodySlot.MessageName = msg.GoIdent.GoName
		bodySlot.FirstCharLowerMessageName = stringhelper.LowerFirstASCII(msg.GoIdent.GoName)
		bodySlot.DisabledAutoCreatedAt = ext.DisabledAutoCreatedAt
		bodySlot.DisabledAutoUpdatedAt = ext.DisabledAutoUpdatedAt
		bodySlot.DisabledAutoExpireAt = ext.DisabledAutoExpireAt
		if bodySlot.ShouldGenModel {
			if ext.ExpireTtlDays == 0 {
				bodySlot.DisabledAutoExpireAt = true
			}
			if !bodySlot.DisabledAutoExpireAt {
				headerSlot.DisabledAutoExpireAt = false
			}
			if !bodySlot.DisabledAutoUpdatedAt {
				headerSlot.DisabledAutoUpdatedAt = false
			}
			if !bodySlot.DisabledAutoCreatedAt {
				headerSlot.DisabledAutoCreatedAt = false
			}
		}
		bodySlot.Cached = ext.Cached
		if bodySlot.ShouldGenModel && bodySlot.Cached {
			headerSlot.Cached = true
		}
		bodySlot.ExpireTtlDays = ext.ExpireTtlDays

		if ext.PrimaryIdFieldName != "" {
			bodySlot.PrimaryIdFieldName = ext.PrimaryIdFieldName
		}

		if ext.OrmModelNameSuffix != "" {
			bodySlot.OrmModelNameSuffix = ext.OrmModelNameSuffix
		}

		if ext.OrmStructNameSuffix != "" {
			bodySlot.OrmStructNameSuffix = ext.OrmStructNameSuffix
		}

		if ext.Conn != "" {
			bodySlot.ConnName = ext.Conn
			indices := parseModelIdxes(ext.Conn)
			if len(indices) > 0 {
				bodySlot.ConnIdxTypes = indices
				bodySlot.HasConnIdxes = true
			}
		}

		if ext.Db != "" {
			bodySlot.DbName = ext.Db
			indices := parseModelIdxes(ext.Db)
			if len(indices) > 0 {
				bodySlot.DbIdxTypes = indices
				bodySlot.HasDbIdxes = true
			}
		} else if bodySlot.ShouldGenModel {
			log.Fatalf("model %s no database used", msg.GoIdent.GoName)
		}

		if ext.Tb != "" {
			bodySlot.TbName = ext.Tb
			indices := parseModelIdxes(ext.Tb)
			if len(indices) > 0 {
				bodySlot.TbIdxTypes = indices
				bodySlot.HasTbIdxes = true
			}
		} else {
			bodySlot.TbName = stringhelper.Snake(msg.GoIdent.GoName)
		}

		mapBsonNameToTopField := make(map[string]*Field)
		for _, field := range bodySlot.Fields {
			bsonName := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(field.BsonTag), ";"), ",omitempty")
			mapBsonNameToTopField[bsonName] = field
		}

		if len(ext.IndexKeys) > 0 {
			bodySlot.IndexKeys = ext.IndexKeys
			idxInfos := parseTopIndexes(ext.IndexKeys, mapBsonNameToTopField)
			bodySlot.TopFieldIdxInfos = idxInfos
		}

		if len(ext.UniqIndexKeys) > 0 {
			bodySlot.UniqIndexKeys = ext.UniqIndexKeys
			idxInfos := parseTopIndexes(ext.UniqIndexKeys, mapBsonNameToTopField)
			bodySlot.TopFieldUniqIdxInfos = idxInfos
		}

		if len(ext.ExpireIndexKeys) > 0 {
			bodySlot.ExpireIndexKeys = ext.ExpireIndexKeys
		}

		if len(bodySlot.ExpireIndexKeys) == 0 && !bodySlot.DisabledAutoExpireAt {
			bodySlot.ExpireIndexKeys = append(bodySlot.ExpireIndexKeys, "expire_at")
		}

		var b bytes.Buffer
		tmpl := template.Must(template.New("bodySection").Funcs(funcMap).Parse(bodySectionTmpl))
		err := tmpl.Execute(&b, bodySlot)
		if err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
		}

		bodySections = append(bodySections, b)
	}

	var b bytes.Buffer
	tmpl := template.Must(template.New("header").Funcs(funcMap).Parse(headerTmpl))
	err := tmpl.Execute(&b, headerSlot)
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	var output bytes.Buffer
	output.WriteString(b.String())
	for _, bodySection := range bodySections {
		output.WriteString(bodySection.String())
	}

	if _, err = plugin.NewGeneratedFile(f.GeneratedFilenamePrefix+"_mgorm.pb.go", f.GoImportPath).Write(output.Bytes()); err != nil {
		log.Println(runtimeutil.NewStackErr(err))
		return err
	}

	return nil
}

func parseTopIndexes(idxKeys []string, mapBsonNameToTopField map[string]*Field) []*IndexInfo {
	var idxInfos []*IndexInfo
	for _, key := range idxKeys {
		comp := strings.Split(key, ":")

		bsonNames := key
		if len(comp) > 1 {
			bsonNames = comp[0]
		}

		bsonNameList := strings.Split(bsonNames, ",")

		var (
			idxFields     []*IndexField
			isAllTopField = true
			fieldNames    []string
		)
		for _, bsonName := range bsonNameList {
			idxField := &IndexField{
				BsonName: bsonName,
			}
			if pos := strings.Index(bsonName, "."); pos >= 0 {
				isAllTopField = false
				log.Println("just gen top field unique index cache key, skip gen cache key for ", bsonNameList)
				break
			} else {
				field, ok := mapBsonNameToTopField[bsonName]
				if !ok {
					isAllTopField = false
					log.Println("just gen top field unique index cache key, skip gen cache key for ", bsonNameList, ",because not found field ", bsonName)
					break
				}

				idxField.Type = field.TypeName
				idxField.Name = field.Name
				fieldNames = append(fieldNames, field.Name)
				idxFields = append(idxFields, idxField)
			}
		}

		if isAllTopField {
			var cacheKeys string
			for _, field := range idxFields {
				if cacheKeys != "" {
					cacheKeys += "_"
				}
				cacheKeys += field.BsonName + ":"
				switch field.Type {
				case "string":
					cacheKeys += "%s"
				case "int", "int32", "int64", "uint", "uint32", "uint64":
					cacheKeys += "%d"
				default:
					cacheKeys += "%v"
				}
			}
			idxInfos = append(idxInfos, &IndexInfo{
				Fields:                  idxFields,
				NamedByJoinAnd:          strings.Join(fieldNames, "And"),
				CacheKeyWithPlaceholder: cacheKeys,
			})
		}
	}

	return idxInfos
}

func parseModelIdxes(modConnProperty string) []string {
	var idxTypes []string
	if modConnProperty != "" {
		indices := stringhelper.FindAllSubstringIdx(modConnProperty, "%")
		if len(indices) > 0 {
			propertyLen := len(modConnProperty)
			for _, idx := range indices {
				if idx+1 >= propertyLen {
					break
				}

				placeholder := modConnProperty[idx+1]

				if placeholder == 's' {
					idxTypes = append(idxTypes, "string")
					continue
				}

				if placeholder == 'd' {
					idxTypes = append(idxTypes, "int64")
					continue
				}
			}
		}
	}
	return idxTypes
}

// registerMessages 用于递归注册 message 到全局映射（含嵌套 message）
func registerMessages(messageMap map[string]*protogen.Message, msg *protogen.Message) {
	fullName := string(msg.Desc.FullName()) // e.g. example.User.Address
	messageMap[fullName] = msg
	for _, nested := range msg.Messages {
		registerMessages(messageMap, nested)
	}
}

// parseMsgFields 解析 proto message 的字段信息
func parseMsgFields(f *protogen.File, headerSlot *TemplateHeaderSlot, bodySlot *TemplateBodySlot, msg *protogen.Message, messageMap map[string]*protogen.Message) []*Field {
	var fields []*Field

	var omitemptyBsonTag, omitemptyJonTag bool
	if proto.HasExtension(msg.Desc.Options(), pb.E_MgormOpts) {
		msgExt := proto.GetExtension(msg.Desc.Options(), pb.E_MgormOpts).(*pb.MgormOptions)
		omitemptyBsonTag = msgExt.OmitemptyDefaultBsonTag
		omitemptyJonTag = msgExt.OmitemptyDefaultJsonTag
	}

	for _, field := range msg.Fields {
		bsonTag := stringhelper.Snake(field.GoName)
		if omitemptyBsonTag {
			bsonTag += ",omitempty"
		}
		jsonTag := stringhelper.Snake(field.GoName)
		if omitemptyJonTag {
			jsonTag += ",omitempty"
		}
		var tags string
		// 支持 mgorm 字段扩展选项
		if proto.HasExtension(field.Desc.Options(), pb.E_MgormFieldOpts) {
			fieldExt := proto.GetExtension(field.Desc.Options(), pb.E_MgormFieldOpts).(*pb.MgormFieldOptions)
			if fieldExt.BsonTag != "" {
				bsonTag = fieldExt.BsonTag
			}
			if fieldExt.JsonTag != "" {
				jsonTag = fieldExt.JsonTag
			}
			tags = fieldExt.Tags
		}

		var typeName string
		// 判断类型（list / map / 基础类型）
		switch {
		case field.Desc.IsList():
			elemType := kindToGoType(f, headerSlot, bodySlot, nil, field.Desc.Kind(), field, messageMap)
			typeName = "[]" + elemType

		case field.Desc.IsMap():
			keyType := kindToGoType(f, headerSlot, bodySlot, nil, field.Desc.MapKey().Kind(), field, messageMap)
			valType := kindToGoType(f, headerSlot, bodySlot, field.Desc.MapValue(), field.Desc.MapValue().Kind(), field, messageMap)
			typeName = fmt.Sprintf("map[%s]%s", keyType, valType)

		default:
			typeName = kindToGoType(f, headerSlot, bodySlot, nil, field.Desc.Kind(), field, messageMap)
		}

		fields = append(fields, &Field{
			Name:     field.GoName,
			TypeName: typeName,
			BsonTag:  bsonTag,
			JsonTag:  jsonTag,
			Tags:     tags,
		})
	}

	return fields
}

// kindToGoType 负责把 proto 类型映射为 Go 类型
func kindToGoType(f *protogen.File, headerSlot *TemplateHeaderSlot, bodySlot *TemplateBodySlot, fd protoreflect.FieldDescriptor, kind protoreflect.Kind, pgField *protogen.Field, messageMap map[string]*protogen.Message) string {
	switch kind {
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.EnumKind:
		if pgField != nil && pgField.Enum != nil {
			importPath := string(pgField.Enum.GoIdent.GoImportPath)
			if importPath != string(f.GoImportPath) {
				extraImportPathSet := make(map[string]struct{})
				for _, imp := range headerSlot.ExtraImportPaths {
					extraImportPathSet[imp] = struct{}{}
				}
				if _, ok := extraImportPathSet[importPath]; !ok {
					headerSlot.ExtraImportPaths = append(headerSlot.ExtraImportPaths, importPath)
					extraImportPathSet[importPath] = struct{}{}
				}
			}
			return pgField.Enum.GoIdent.GoName
		}
		if fd != nil && fd.Enum() != nil {
			return string(fd.Enum().Name())
		}
		return "int32"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "uint32"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if pgField != nil {
			if proto.HasExtension(pgField.Desc.Options(), pb.E_MgormFieldOpts) {
				ext := proto.GetExtension(pgField.Desc.Options(), pb.E_MgormFieldOpts).(*pb.MgormFieldOptions)
				if ext.FieldType == pb.FieldType_FieldTypeDatetime {
					headerSlot.ShouldImportTime = true
					return "time.Time"
				}
			}
		}
		return "uint64"
	case protoreflect.FloatKind:
		return "float32"
	case protoreflect.DoubleKind:
		return "float64"
	case protoreflect.StringKind:
		if pgField != nil {
			if proto.HasExtension(pgField.Desc.Options(), pb.E_MgormFieldOpts) {
				ext := proto.GetExtension(pgField.Desc.Options(), pb.E_MgormFieldOpts).(*pb.MgormFieldOptions)
				if ext.FieldType == pb.FieldType_FieldTypeObjectId {
					headerSlot.ShouldImportPrimitive = true
					return "primitive.ObjectID"
				}
			}
		}
		return "string"
	case protoreflect.BytesKind:
		return "[]byte"

	case protoreflect.MessageKind:
		var typeName string
		// 优先使用 pgField.Message
		if pgField != nil && pgField.Message != nil {
			typeName = pgField.Message.GoIdent.GoName
			importPath := string(pgField.Message.GoIdent.GoImportPath)
			field := pgField

			if pgField.Desc.IsMap() {
				// 字段的描述中：
				//
				// 当field.Desc.IsMap() → true
				//
				// field.Message.Fields[0] → key 字段
				//
				// field.Message.Fields[1] → value 字段
				valField := pgField.Message.Fields[1]
				field = valField

				if valField.Message != nil {
					fullName := string(valField.Message.Desc.FullName())
					if m, ok := messageMap[fullName]; ok {
						typeName = m.GoIdent.GoName
						importPath = string(m.GoIdent.GoImportPath)
					} else {
						// fallback，正常情况下不可能执行到这里
						typeName = valField.Message.GoIdent.GoName
						importPath = string(f.GoImportPath)
					}
				} else {
					// fallback，正常情况下不可能执行到这里，因为一定是message类型
					typeName = valField.Desc.Kind().String()
					importPath = ""
				}
			}

			if importPath != string(f.GoImportPath) {
				extraImportPathSet := make(map[string]struct{})
				for _, imp := range headerSlot.ExtraImportPaths {
					extraImportPathSet[imp] = struct{}{}
				}
				if _, ok := extraImportPathSet[importPath]; !ok {
					headerSlot.ExtraImportPaths = append(headerSlot.ExtraImportPaths, importPath)
					extraImportPathSet[importPath] = struct{}{}
				}
				pkgName := path.Base(importPath)
				typeName = pkgName + "." + typeName
			} else { // 处理内嵌结构
				isNestedMessage := true
				for _, topMessage := range f.Messages {
					if topMessage == field.Message {
						isNestedMessage = false
						break
					}
				}

				if isNestedMessage {
					nestedMessageName := typeName + bodySlot.OrmStructNameSuffix

					if !proto.HasExtension(field.Message.Desc.Options(), pb.E_MgormOpts) {
						typeName = nestedMessageName
					}

					var exist bool
					for _, nestedMessage := range bodySlot.NestedMessages {
						if nestedMessage.MessageName == nestedMessageName {
							exist = true
							break
						}
					}

					if !exist {
						bodySlot.NestedMessages = append(bodySlot.NestedMessages, &NestedMessage{
							MessageName: nestedMessageName,
							Fields:      parseMsgFields(f, headerSlot, bodySlot, field.Message, messageMap),
						})
					}
				}
			}

			if proto.HasExtension(field.Message.Desc.Options(), pb.E_MgormOpts) {
				typeName = typeName + bodySlot.OrmStructNameSuffix
			}

			return "*" + typeName
		}

		// 如果只有 descriptor（map value 等场景）
		if fd != nil && fd.Message() != nil {
			fullName := string(fd.Message().FullName())
			if m, ok := messageMap[fullName]; ok {
				typeName = m.GoIdent.GoName
				importPath := string(m.GoIdent.GoImportPath)

				if importPath != string(f.GoImportPath) {
					extraImportPathSet := make(map[string]struct{})
					for _, imp := range headerSlot.ExtraImportPaths {
						extraImportPathSet[imp] = struct{}{}
					}
					if _, ok = extraImportPathSet[importPath]; !ok {
						headerSlot.ExtraImportPaths = append(headerSlot.ExtraImportPaths, importPath)
						extraImportPathSet[importPath] = struct{}{}
					}
					pkgName := path.Base(string(m.GoIdent.GoImportPath))
					typeName = pkgName + "." + typeName
				} else {
					isNestedMessage := true
					for _, topMessage := range f.Messages {
						if topMessage == m {
							isNestedMessage = false
							break
						}
					}

					if isNestedMessage {
						nestedMessageName := typeName + bodySlot.OrmStructNameSuffix

						if !proto.HasExtension(m.Desc.Options(), pb.E_MgormOpts) {
							typeName = nestedMessageName
						}

						var exist bool
						for _, nestedMessage := range bodySlot.NestedMessages {
							if nestedMessage.MessageName == nestedMessageName {
								exist = true
								break
							}
						}

						if !exist {
							bodySlot.NestedMessages = append(bodySlot.NestedMessages, &NestedMessage{
								MessageName: nestedMessageName,
								Fields:      parseMsgFields(f, headerSlot, bodySlot, m, messageMap),
							})
						}
					}
				}

				if proto.HasExtension(m.Desc.Options(), pb.E_MgormOpts) {
					typeName = typeName + bodySlot.OrmStructNameSuffix
				}

				return "*" + typeName
			}

			// fallback
			return "*" + string(fd.Message().Name())
		}

		return "any"
	default:
		return "any"
	}
}
