const fs = require('fs')
const yaml = require('E:/trustkernel/workspace/tinfoil-webapp/node_modules/yaml')
const root = __dirname
const spec = yaml.parse(fs.readFileSync(`${root}/openapi.yaml`, 'utf8'))
const file = `${root}/openapi.zh-CN.md`
const names = {
  Json:'任意 JSON 值',JsonObject:'JSON 对象',Identifier:'标识符',Timestamp:'时间戳',Keyed:'携带内容密钥的请求',KeyedId:'携带密钥与 ID 的请求',ThreadIdRequest:'聊天 ID 请求',PageRequest:'分页请求',RemoteImage:'远程图片',ErrorResponse:'错误响应',ErrorBody:'错误详情',ErrorAction:'错误处理动作',ErrorCode:'错误码',Session:'应用会话',SessionModel:'会话模型',Preset:'预设',Composer:'输入框能力',FileGroup:'文件类别',RateLimit:'额度信息',TokenBudget:'Token 预算',Turn:'聊天操作',BrowserAccess:'浏览器执行权限',BrowserAction:'浏览器操作',BrowserResult:'浏览器执行结果',BrowserResultInput:'浏览器结果提交',BrowserToolRequestEvent:'浏览器工具请求事件',TurnOptions:'聊天操作选项',SentAttachment:'发送的附件',FollowInput:'续接输入',RunRequest:'运行操作请求',HarnessEvent:'运行事件联合类型',RunStartedEvent:'运行开始事件',MessagesSnapshotEvent:'消息快照事件',StateSnapshotEvent:'状态快照事件',StateDeltaEvent:'状态增量事件',StatePatch:'状态补丁',QueueItem:'排队操作',TextMessageChunkEvent:'文本增量事件',ReasoningMessageChunkEvent:'思考增量事件',ReasoningSummaryEvent:'思考摘要事件',ToolCallStartEvent:'工具开始事件',ToolCallArgsEvent:'工具参数事件',ToolCallEndEvent:'工具参数结束事件',ToolCallResultEvent:'工具结果事件',ActivitySnapshotEvent:'活动快照事件',ActivityDeltaEvent:'活动增量事件',ToolActivity:'工具活动',ActivityPatch:'活动补丁',CompactionStartEvent:'压缩开始事件',CompactionEndEvent:'压缩结束事件',RunFinishedEvent:'运行结束事件',RunErrorEvent:'运行错误事件',RunMetadata:'运行元数据',CommittedReply:'已提交回复',TitleState:'标题状态',ContextUsage:'上下文用量',ThreadSummary:'聊天摘要',ThreadWindow:'聊天消息窗口',MessageView:'消息视图',Research:'深度研究信息',Compaction:'压缩信息',WidgetAnswers:'组件回答',AnsweredQuestion:'已回答问题',QuestionOption:'问题选项',Role:'消息角色',MessageUsage:'消息用量',Block:'回复时间线步骤',ToolDisplay:'工具展示数据',Annotation:'引用标注',AttachmentKind:'附件类型',AttachmentView:'附件视图',Attachment:'附件记录',DocumentPage:'文档页面',ThreadRecord:'聊天持久化记录',MessageRecord:'消息持久化记录',ThreadInput:'设备聊天输入',MessageRequest:'消息操作请求',DeviceThreadReply:'设备聊天返回',SharedThread:'分享聊天',AttachmentRequest:'附件请求',Profile:'账户设置',ProfileView:'账户设置视图',ProfileInput:'账户设置输入',CustomPreset:'自定义预设',PresetUpdate:'预设更新',Project:'项目',ProjectDocument:'项目文档',KeyOrigin:'密钥来源',KeyRef:'密钥引用',KeyBundleInput:'密钥包输入',KeyCurrent:'当前内容密钥',ResealReport:'重新封存报告',ImportLine:'导入结果行',ImportStatus:'导入任务状态',LinkPreview:'链接预览'
}
const fields = {
  key:'内容密钥',id:'标识符',threadId:'聊天 ID',runId:'运行 ID',clientRequestId:'幂等请求 ID',kind:'操作或类别',content:'内容',quote:'引用内容',messageId:'消息 ID',toolCallId:'工具调用 ID',attachments:'附件',widgets:'组件',options:'本轮选项',model:'模型 ID',webSearch:'联网搜索开关',piiCheck:'PII 检查开关',genUI:'生成式界面开关',research:'深度研究',cacheSecret:'缓存隔离密钥',local:'设备本地聊天',thread:'聊天记录',profile:'账户设置',projectId:'项目 ID',presetId:'预设 ID',timestamp:'事件时间',type:'事件类型',delta:'增量内容',messages:'消息列表',hasOlder:'是否还有更早消息',snapshot:'完整快照',metadata:'元数据',status:'状态',error:'错误',code:'错误码',message:'错误说明',action:'处理动作',retryAfter:'建议等待秒数',resetsAt:'额度恢复时间',rateLimit:'额度信息',title:'标题',titleState:'标题状态',createdAt:'创建时间',updatedAt:'更新时间',activityAt:'最近活动时间',role:'消息角色',timeline:'回复时间线',usage:'用量',name:'名称',description:'说明',url:'网址',query:'查询词',result:'结果',progress:'进度',display:'展示信息',instance:'运行所在实例',queueId:'排队项 ID',recordVersion:'记录版本',revision:'修订版本',activeRun:'当前运行',contextUsage:'上下文用量',inputTokens:'输入 Token 数',outputTokens:'输出 Token 数',promptTokens:'提示词 Token 数',completionTokens:'生成 Token 数',remaining:'剩余额度',max:'上限',used:'已使用',expiresAt:'过期时间',executionId:'浏览器操作 ID',executorId:'浏览器执行者 ID',token:'执行令牌',operation:'具体操作',text:'文字',image:'图片',fileName:'文件名',filename:'文件名',mimeType:'媒体类型',size:'文件大小',pages:'文档页面',base64:'Base64 数据',thumbnail:'缩略图',textContent:'提取的文字',annotations:'引用标注',selectedModel:'已选模型',webSearchEnabled:'聊天的联网搜索设置',webSearchAvailable:'联网搜索可用状态',features:'服务端能力开关',models:'模型目录',defaultModel:'默认模型',presets:'预设列表',user:'用户信息',protocol:'协议版本',locale:'语言区域',timezone:'时区',thinking:'思考开关',reasoningEffort:'思考强度',autoIntelligence:'自动智能等级',compactLatest:'连最近一轮一起压缩',ephemeral:'不保存的临时操作',cancelled:'是否取消',truncated:'是否截断',pinned:'是否置顶',flagged:'是否标记',source:'来源',sources:'来源列表',keyId:'密钥 ID',upstreamCode:'上游错误码'
}
Object.assign(fields, {
  accept:'接受的文件类型',activity:'活动信息',activityType:'活动类型',additionalContext:'额外上下文',answered:'是否已回答',answers:'问题回答',arguments:'工具参数',attKey:'附件密钥',auto:'自动模式',badges:'模型标记',beforeMessageId:'压缩边界消息 ID',blocked:'受阻数量',blockId:'内容块 ID',browser:'浏览器执行配置',browserTabChatTitleEnabled:'浏览器标签页显示聊天标题',builtIn:'是否内置',bundles:'密钥包',chatFont:'聊天字体',color:'颜色',compaction:'压缩信息',complete:'是否完成',composer:'输入框能力',contentType:'内容类型',counts:'分类计数',created_at:'创建时间',created_via:'创建方式',credential_id:'Passkey 凭证 ID',customPromptPresets:'自定义提示词预设',customSystemPrompt:'自定义系统提示词',defaultPromptPresetId:'默认提示词预设 ID',deprecated:'是否已弃用',dictationMaxSeconds:'最长语音输入秒数',dictationSegmentSeconds:'语音输入分段秒数',direction:'方向',dismissed:'已关闭的提示',done:'完成信息',duration:'持续时间',encrypted_keys:'加密后的密钥',endedAt:'结束时间',enterToNewlineEnabled:'回车换行开关',errors:'错误列表',etag:'版本标识',failed:'失败数量',failureDetail:'失败详情',failureReason:'失败原因',favoritePromptPresetIds:'收藏的提示词预设 ID',genUIEnabled:'生成式界面开关',harnessHiddenCount:'harness 隐藏消息数量',has_data:'是否已有数据',hasSeenOnboarding:'是否看过新手引导',hasSeenWebSearchIntro:'是否看过联网搜索介绍',icon:'图标',image_bytes:'图片字节数据',image_content_type:'图片媒体类型',imageMaxSide:'图片最长边限制',imported:'已导入数量',invalidArguments:'工具参数是否无效',is_scanned:'是否扫描件',isError:'是否错误',isInterrupted:'是否中断',isUsingCustomPrompt:'是否使用自定义提示词',isUsingPersonalization:'是否使用个性化设置',jobId:'任务 ID',kek_iv:'密钥加密密钥的初始化向量',key_id:'密钥 ID',label:'显示名称',language:'语言',lastRequestId:'上次请求 ID',lastRequestKind:'上次请求类型',limitTokens:'上下文 Token 上限',marker:'压缩标记',maxBytes:'最大字节数',maxRequests:'最大请求次数',messageCount:'消息数量',migrated:'已迁移数量',modelDisplayName:'模型显示名称',nickname:'昵称',originalBase64:'原始文件 Base64 数据',outcome:'处理结果',output:'输出内容',page:'页码',parentMessageId:'所属消息 ID',partial:'是否部分完成',pasteAsFileChars:'粘贴转文件的字符阈值',patch:'增量补丁',percentage:'百分比',phase:'任务阶段',piiCheckEnabled:'PII 检查开关',pixelateSidebarChatTitlesEnabled:'侧栏标题模糊显示开关',profession:'职业',profileDefaults:'账户设置默认值',projectDocuments:'项目文档配置',prompt:'提示词',questions:'问题列表',readAloudSpeed:'朗读速度',reasoning:'思考能力',ref:'元素引用',screenshotId:'截图 ID',selected:'所选选项',shortLabel:'简短名称',siteName:'网站名称',sizeBytes:'文件大小（字节）',startedAt:'开始时间',systemInstructions:'项目系统指令',systemPrompt:'系统提示词',tabId:'浏览器标签页 ID',themeMode:'主题模式',thinkingEnabled:'思考开关',thumbnailBase64:'缩略图 Base64 数据',tool:'工具',toolCallName:'工具名称',total:'总数',traits:'个性化特征',url_citation:'网址引用',usedTokens:'已使用 Token 数',version:'版本',warnings:'警告列表',x:'横坐标',y:'纵坐标'
})
function typeOf(s) {
  if (!s) return '未指定'
  if (s.$ref) return `\`${s.$ref.split('/').at(-1)}\``
  if (s.const !== undefined) return `固定值 \`${s.const}\``
  if (s.enum) return s.enum.map(x => `\`${String(x)}\``).join('、')
  if (s.anyOf) return s.anyOf.map(typeOf).join(' / ')
  if (s.oneOf) return s.oneOf.map(typeOf).join(' / ')
  if (s.allOf) return s.allOf.map(typeOf).join(' + ')
  if (Array.isArray(s.type)) return s.type.map(x => x === 'null' ? 'null' : typeOf({type:x})).join(' / ')
  if (s.type === 'array') return `数组〈${typeOf(s.items)}〉`
  return ({string:'字符串',integer:'整数',number:'数字',boolean:'布尔值',object:'对象',null:'null'})[s.type] || '任意 JSON'
}
const out = ['\n## 附录：全部 Schema 字段速查\n', '本附录从 `openapi.yaml` 自动提取字段、必填性、类型和枚举；“含义”是中文速记。具体长度、格式、条件和业务限制仍以原始 YAML 为准。\n']
for (const [name, s] of Object.entries(spec.components.schemas)) {
  out.push(`### \`${name}\`：${names[name] || name}\n`)
  const props = s.properties || {}
  if (!Object.keys(props).length) {
    out.push(`类型：${typeOf(s)}。\n`)
    continue
  }
  out.push('| 字段 | 必填 | 类型或允许值 | 含义 |', '| --- | --- | --- | --- |')
  for (const [field, def] of Object.entries(props)) {
    const required = (s.required || []).includes(field) ? '是' : '否'
    out.push(`| \`${field}\` | ${required} | ${typeOf(def)} | ${fields[field] || '参见原始 Schema 说明'} |`)
  }
  out.push('')
}
fs.appendFileSync(file, out.join('\n') + '\n')
